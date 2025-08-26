package main

import (
	"database/sql"
	"errors"
	"fmt"
)

var (
	// ErrNoDBConnection indicates that the store has not been
	// initialised with a valid *sql.DB connection.
	ErrNoDBConnection = errors.New("no database connection")

	// Business logic errors
	ErrNewStatusUnrecognised    = errors.New("unrecognised new status")
	ErrStoredStatusUnrecognised = errors.New("unrecognised stored status")
	ErrInvalidStatusTransition  = errors.New("invalid status transition")
	ErrRequireRegistered        = errors.New("requires registered status")
)

// ParcelStore wraps a *sql.DB handle and provides higher–level
// CRUD operations for the "parcel" table.
//
// Exported methods on ParcelStore check for a nil database connection
// before executing queries and return ErrNoDBConnection if
// the store has not been properly initialised.
type ParcelStore struct {
	db *sql.DB
}

// Add inserts a new parcel record into the database using the values
// from the provided Parcel struct (client, status, address, created_at).
//
// Behavior:
//   - Returns ErrNoDBConnection if the store has not been initialised.
//   - Returns ErrNewStatusUnrecognised if the status is not one of
//     ("registered", "sent", "delivered").
//   - Inserts a new row into the "parcel" table with the given values.
//   - Returns the generated parcel number on success.
//   - Wraps and returns any SQL errors from INSERT or ID retrieval.
func (s ParcelStore) Add(p Parcel) (int, error) {
	if s.db == nil {
		return 0, ErrNoDBConnection
	}

	if p.Status != ParcelStatusDelivered && p.Status != ParcelStatusRegistered && p.Status != ParcelStatusSent {
		return 0, fmt.Errorf("failed to add parcel for client %d: %w %q", p.Client, ErrNewStatusUnrecognised, p.Status)
	}

	query := `INSERT INTO parcel (client, status, address, created_at)
VALUES (:client, :status, :address, :created_at)`
	res, err := s.db.Exec(query, sql.Named("client", p.Client), sql.Named("status", p.Status),
		sql.Named("address", p.Address), sql.Named("created_at", p.CreatedAt))
	if err != nil {
		return 0, fmt.Errorf("failed to add parcel for client %d: %w", p.Client, err)
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to get id of added parcel for client %d: %w", p.Client, err)
	}
	return int(id), nil
}

// Get retrieves a single parcel by its unique number (primary key).
//
// Behavior:
//   - Returns ErrNoDBConnection if the store is not initialised.
//   - Executes a SELECT query against "parcel" by primary key.
//   - Returns sql.ErrNoRows (wrapped) if no matching parcel exists.
//   - Returns a fully populated Parcel struct on success.
//   - Wraps and returns any SQL errors from query execution or scanning.
func (s ParcelStore) Get(number int) (Parcel, error) {
	var p Parcel

	if s.db == nil {
		return p, ErrNoDBConnection
	}

	query := "SELECT number, client, status, address, created_at FROM parcel WHERE number = :number"
	row := s.db.QueryRow(query, sql.Named("number", number))
	err := row.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
	if err != nil {
		return p, fmt.Errorf("failed to scan parcel row with number %d: %w", number, err)
	}
	return p, nil
}

// GetByClient retrieves all parcels belonging to the specified client ID.
//
// Behavior:
//   - Returns ErrNoDBConnection if the store is not initialised.
//   - Executes a SELECT query against "parcel" filtered by client.
//   - Returns an empty slice if the client has no parcels.
//   - Wraps and returns any SQL errors from query, row scanning, or iteration.
//   - Always closes the cursor after use.
func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {
	var res []Parcel

	if s.db == nil {
		return res, ErrNoDBConnection
	}

	query := "SELECT number, client, status, address, created_at FROM parcel WHERE client = :client"
	rows, err := s.db.Query(query, sql.Named("client", client))
	if err != nil {
		return res, fmt.Errorf("failed to get cursor for result of client %d: %w", client, err)
	}
	defer rows.Close()

	for rows.Next() {
		var p Parcel

		err := rows.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan one of parcel rows for client %d: %w", client, err)
		}
		res = append(res, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate parcel rows for client %d: %w", client, err)
	}
	return res, nil
}

// SetStatus updates the status of a parcel identified by its number.
//
// The transition is only permitted if the new status represents the next
// valid step in the delivery lifecycle:
//
//	registered → sent → delivered
//
// Behaviour:
//   - If the store has not been initialised with a database connection,
//     ErrNoDBConnection is returned.
//   - If the supplied new status is not recognised, ErrNewStatusUnrecognised
//     is returned (wrapped with context).
//   - If the stored status in the database is not recognised,
//     ErrStoredStatusUnrecognised is returned (wrapped).
//   - If the attempted change does not represent a valid forward transition
//     (e.g. delivered → sent, or skipping steps),
//     ErrInvalidStatusTransition is returned (wrapped).
//   - On any database execution failure, the underlying error is wrapped
//     with context.
//
// Records with unrecognised statuses are considered invalid and should be
// corrected or removed manually before retrying.
func (s ParcelStore) SetStatus(number int, status string) error {
	if s.db == nil {
		return ErrNoDBConnection
	}

	storedStatus, err := s.getStatus(number)
	if err != nil {
		return err
	}
	var statusOrder = map[string]int{
		ParcelStatusRegistered: 0,
		ParcelStatusSent:       1,
		ParcelStatusDelivered:  2,
	}
	statusRank, ok := statusOrder[status]
	if !ok {
		return fmt.Errorf("failed to update status: %w %q for parcel with number %d", ErrNewStatusUnrecognised, status, number)
	}
	storedStatusRank, ok := statusOrder[storedStatus]
	if !ok {
		return fmt.Errorf("failed to update status: %w %q for parcel with number %d", ErrStoredStatusUnrecognised, storedStatus, number)
	}
	if statusRank-storedStatusRank != 1 {
		return fmt.Errorf("failed to update status: %w %q → %q for parcel with number %d", ErrInvalidStatusTransition, storedStatus, status, number)
	}

	query := "UPDATE parcel SET status = :status WHERE number = :number"
	_, err = s.db.Exec(query, sql.Named("status", status), sql.Named("number", number))
	if err != nil {
		return fmt.Errorf("failed to update status %q to %q for parcel with number %d: %w", storedStatus, status, number, err)
	}
	return nil
}

// SetAddress updates the delivery address of a parcel identified by its number.
//
// The update is only permitted if the parcel’s current status is `registered`.
// Attempting to update the address after the parcel has been sent or delivered
// results in an error.
//
// Behaviour:
//   - If the store has not been initialised with a database connection,
//     ErrNoDBConnection is returned.
//   - If the stored status is not `registered`, ErrRequireRegistered is returned
//     (wrapped with context).
//   - On database execution failure, the underlying error is wrapped with context.
func (s ParcelStore) SetAddress(number int, address string) error {
	if s.db == nil {
		return ErrNoDBConnection
	}

	storedStatus, err := s.getStatus(number)
	if err != nil {
		return err
	}
	if storedStatus != ParcelStatusRegistered {
		return fmt.Errorf("failed to update address: %w (parcel %d has status %q)", ErrRequireRegistered, number, storedStatus)
	}

	queryUpdate := "UPDATE parcel SET address = :address WHERE number = :number"
	_, err = s.db.Exec(queryUpdate, sql.Named("address", address), sql.Named("number", number))
	if err != nil {
		return fmt.Errorf("failed to update address for parcel with number %d: %w", number, err)
	}
	return nil
}

// Delete removes a parcel identified by its number from the database.
//
// Deletion is only permitted if the parcel’s current status is `registered`.
// Attempting to delete a parcel that has already been sent or delivered
// results in an error.
//
// Behaviour:
//   - If the store has not been initialised with a database connection,
//     ErrNoDBConnection is returned.
//   - If the stored status is not `registered`, ErrRequireRegistered is returned
//     (wrapped with context).
//   - On database execution failure, the underlying error is wrapped with context.
func (s ParcelStore) Delete(number int) error {
	if s.db == nil {
		return ErrNoDBConnection
	}

	storedStatus, err := s.getStatus(number)
	if err != nil {
		return err
	}
	if storedStatus != ParcelStatusRegistered {
		return fmt.Errorf("failed to delete parcel: %w (parcel %d has status %q)", ErrRequireRegistered, number, storedStatus)
	}

	queryDelete := "DELETE FROM parcel WHERE number = :number"
	_, err = s.db.Exec(queryDelete, sql.Named("number", number))
	if err != nil {
		return fmt.Errorf("failed to delete parcel with number %d: %w", number, err)
	}
	return nil
}

// getStatus retrieves the current status of a parcel by its number.
//
// It queries only the `status` column for efficiency. Used internally
// by SetStatus, SetAddress, and Delete to check whether an operation
// is allowed. Errors from Scan are wrapped with context.
func (s ParcelStore) getStatus(number int) (string, error) {
	var storedStatus string

	querySelect := "SELECT status FROM parcel WHERE number = :number"
	row := s.db.QueryRow(querySelect, sql.Named("number", number))
	err := row.Scan(&storedStatus)
	if err != nil {
		return "", fmt.Errorf("failed to scan parcel row with number %d: %w", number, err)
	}
	return storedStatus, nil
}

// NewParcelStore returns a new ParcelStore bound to the provided *sql.DB.
func NewParcelStore(db *sql.DB) ParcelStore {
	return ParcelStore{db: db}
}
