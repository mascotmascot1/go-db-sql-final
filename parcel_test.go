package main

import (
	"database/sql"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testSchema = `CREATE TABLE IF NOT EXISTS "parcel" (
    number INTEGER PRIMARY KEY AUTOINCREMENT,
    client INTEGER NOT NULL,
    status VARCHAR(128) NOT NULL,
    address VARCHAR(512) NOT NULL,
    created_at VARCHAR(64) NOT NULL
);
CREATE INDEX parcel_client ON parcel(client);
CREATE INDEX parcel_created_at ON parcel(created_at);
`

var (
	// randSource is a pseudo-random number generator.
	// It is seeded with the current Unix timestamp for higher uniqueness.
	randSource = rand.NewSource(time.Now().UnixNano())
	// randRange uses randSource to generate random numbers.
	randRange = rand.New(randSource)
)

// getTestParcel returns a sample test parcel.
func getTestParcel() Parcel {
	return Parcel{
		Client:    1000,
		Status:    ParcelStatusRegistered,
		Address:   "test",
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}
}

// getTestDB creates and returns an in-memory SQLite database for testing.
// Marked as helper (t.Helper()), so errors are reported at the caller level.
func getTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)

	_, err = db.Exec(testSchema)
	require.NoError(t, err)
	return db
}

// TestAddGetDeleteWhenValidStatus verifies adding, retrieving and deleting
// a parcel with a valid status.
func TestAddGetDeleteWhenValidStatus(t *testing.T) {
	// prepare
	db := getTestDB(t)
	defer db.Close()
	store, parcel := NewParcelStore(db), getTestParcel()
	parcel.Status = ParcelStatusRegistered

	// add
	id, err := store.Add(parcel)
	require.NoError(t, err)
	require.NotEmpty(t, id)
	parcel.Number = id

	// get
	storedParcel, err := store.Get(id)
	require.NoError(t, err)
	assert.Equal(t, parcel, storedParcel)

	// delete
	err = store.Delete(id)
	require.NoError(t, err)

	afterDelete, err := store.Get(id)
	require.ErrorIs(t, err, sql.ErrNoRows)
	require.Empty(t, afterDelete)
}

// TestAddGetDeleteWhenInvalidStatus verifies behaviour when attempting to
// delete a parcel with an invalid status.
func TestAddGetDeleteWhenInvalidStatus(t *testing.T) {
	// prepare
	db := getTestDB(t)
	defer db.Close()
	store, parcel := NewParcelStore(db), getTestParcel()
	parcel.Status = ParcelStatusDelivered

	// add
	id, err := store.Add(parcel)
	require.NoError(t, err)
	require.NotEmpty(t, id)
	parcel.Number = id

	// get
	storedParcel, err := store.Get(id)
	require.NoError(t, err)
	assert.Equal(t, parcel, storedParcel)

	// delete
	err = store.Delete(id)
	require.ErrorIs(t, err, ErrRequireRegistered)

	afterDelete, err := store.Get(id)
	require.NoError(t, err)
	assert.Equal(t, parcel, afterDelete)
}

// TestAddWhenUnrecognisedNewStatus ensures that adding a parcel
// with an unrecognised status fails.
func TestAddWhenUnrecognisedNewStatus(t *testing.T) {
	// prepare
	db := getTestDB(t)
	defer db.Close()
	store, parcel := NewParcelStore(db), getTestParcel()
	parcel.Status = "unrecognised"

	// add
	id, err := store.Add(parcel)
	require.ErrorIs(t, err, ErrNewStatusUnrecognised)
	require.Empty(t, id)

	// check
	var actualCount int = -1

	query := "SELECT COUNT(*) FROM parcel"
	row := store.db.QueryRow(query)
	err = row.Scan(&actualCount)

	require.NoError(t, err)
	require.Zero(t, actualCount)
}

// TestSetAddressWhenValidStatus verifies updating a parcel address
// when the status is valid.
func TestSetAddressWhenValidStatus(t *testing.T) {
	// prepare
	db := getTestDB(t)
	defer db.Close()
	store, parcel := NewParcelStore(db), getTestParcel()
	parcel.Status = ParcelStatusRegistered

	// add
	id, err := store.Add(parcel)
	require.NoError(t, err)
	require.NotEmpty(t, id)

	// set address
	newAddress := "new test address"
	err = store.SetAddress(id, newAddress)
	require.NoError(t, err)

	// check
	storedParcel, err := store.Get(id)
	require.NoError(t, err)
	require.Equal(t, newAddress, storedParcel.Address)
}

// TestSetAddressWhenInvalidStatus ensures that updating the address fails
// if the parcel is not in `registered` status.
func TestSetAddressWhenInvalidStatus(t *testing.T) {
	// prepare
	db := getTestDB(t)
	defer db.Close()
	store, parcel := NewParcelStore(db), getTestParcel()
	parcel.Status = ParcelStatusSent

	// add
	id, err := store.Add(parcel)
	require.NoError(t, err)
	require.NotEmpty(t, id)

	// set address
	newAddress := "new test address"
	err = store.SetAddress(id, newAddress)
	require.ErrorIs(t, err, ErrRequireRegistered)

	// check
	storedParcel, err := store.Get(id)
	require.NoError(t, err)
	require.Equal(t, parcel.Address, storedParcel.Address)
}

// TestSetStatusValidTransition verifies that a valid status
// transition is applied successfully.
func TestSetStatusValidTransition(t *testing.T) {
	// prepare
	db := getTestDB(t)
	defer db.Close()
	store, parcel := NewParcelStore(db), getTestParcel()
	parcel.Status = ParcelStatusRegistered

	// add
	id, err := store.Add(parcel)
	require.NoError(t, err)
	require.NotEmpty(t, id)

	// set status
	newStatus := ParcelStatusSent
	err = store.SetStatus(id, newStatus)
	require.NoError(t, err)

	// check
	storedParcel, err := store.Get(id)
	require.NoError(t, err)
	require.Equal(t, newStatus, storedParcel.Status)
}

// TestSetStatusInvalidTransition checks that an invalid status transition
// returns an error and does not update the parcel.
func TestSetStatusInvalidTransition(t *testing.T) {
	// prepare
	db := getTestDB(t)
	defer db.Close()
	store, parcel := NewParcelStore(db), getTestParcel()
	parcel.Status = ParcelStatusRegistered

	// add
	id, err := store.Add(parcel)
	require.NoError(t, err)
	require.NotEmpty(t, id)

	// set status
	invalidStatus := ParcelStatusDelivered
	err = store.SetStatus(id, invalidStatus)
	require.ErrorIs(t, err, ErrInvalidStatusTransition)

	// check
	storedParcel, err := store.Get(id)
	require.NoError(t, err)
	require.Equal(t, parcel.Status, storedParcel.Status)
}

// TestSetStatusWhenUnrecognisedNewStatus ensures that setting
// an unrecognised status fails.
func TestSetStatusWhenUnrecognisedNewStatus(t *testing.T) {
	// prepare
	db := getTestDB(t)
	defer db.Close()
	store, parcel := NewParcelStore(db), getTestParcel()
	parcel.Status = ParcelStatusRegistered

	// add
	id, err := store.Add(parcel)
	require.NoError(t, err)
	require.NotEmpty(t, id)

	// set status
	unrecognisedStatus := "unrecognised"
	err = store.SetStatus(id, unrecognisedStatus)
	require.ErrorIs(t, err, ErrNewStatusUnrecognised)

	// check
	storedParcel, err := store.Get(id)
	require.NoError(t, err)
	require.Equal(t, parcel.Status, storedParcel.Status)
}

// TestSetStatusWhenUnrecognisedStoredStatus ensures that updating a parcel
// with an unrecognised stored status fails.
func TestSetStatusWhenUnrecognisedStoredStatus(t *testing.T) {
	// prepare
	db := getTestDB(t)
	defer db.Close()
	store, parcel := NewParcelStore(db), getTestParcel()
	parcel.Status = "unrecognised"

	// add
	query := `INSERT INTO parcel (client, status, address, created_at)
VALUES (:client, :status, :address, :created_at)`
	res, err := store.db.Exec(query, sql.Named("client", parcel.Client), sql.Named("status", parcel.Status),
		sql.Named("address", parcel.Address), sql.Named("created_at", parcel.CreatedAt))
	require.NoError(t, err)

	id, err := res.LastInsertId()
	require.NoError(t, err)
	require.NotEmpty(t, id)

	// set status
	newStatus := ParcelStatusRegistered
	err = store.SetStatus(int(id), newStatus)
	require.ErrorIs(t, err, ErrStoredStatusUnrecognised)

	// check
	storedParcel, err := store.Get(int(id))
	require.NoError(t, err)
	require.Equal(t, parcel.Status, storedParcel.Status)
}

// TestGetByClient verifies retrieving parcels by client ID.
func TestGetByClient(t *testing.T) {
	// prepare
	db := getTestDB(t)
	defer db.Close()
	store := NewParcelStore(db)

	parcels := []Parcel{
		getTestParcel(),
		getTestParcel(),
		getTestParcel(),
	}
	parcelMap := map[int]Parcel{}

	// assign the same client ID to all parcels
	client := randRange.Intn(10_000_000)
	parcels[0].Client = client
	parcels[1].Client = client
	parcels[2].Client = client

	// add
	for i := 0; i < len(parcels); i++ {
		id, err := store.Add(parcels[i])
		require.NoError(t, err)
		require.NotEmpty(t, id)

		// update parcel ID
		parcels[i].Number = id

		// save added parcel into a map for quick lookup
		parcelMap[id] = parcels[i]
	}

	// get by client
	storedParcels, err := store.GetByClient(client)
	require.NoError(t, err)
	require.Len(t, storedParcels, len(parcels))

	// check
	for _, storedParcel := range storedParcels {
		localParcel, ok := parcelMap[storedParcel.Number]
		assert.True(t, ok)
		assert.Equal(t, localParcel, storedParcel)
	}
}
