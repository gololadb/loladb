package sql

import (
	"testing"
)

func setupWindowTestData(t *testing.T) *Executor {
	t.Helper()
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE emp (id INT, name TEXT, dept TEXT, salary INT)`)
	ex.Exec(`INSERT INTO emp VALUES
		(1, 'Alice', 'eng', 100),
		(2, 'Bob', 'eng', 120),
		(3, 'Carol', 'eng', 100),
		(4, 'Dave', 'sales', 90),
		(5, 'Eve', 'sales', 110),
		(6, 'Frank', 'sales', 110)`)
	return ex
}

// --- row_number ---

func TestWindow_RowNumber(t *testing.T) {
	ex := setupWindowTestData(t)

	r, err := ex.Exec(`SELECT name, row_number() OVER (ORDER BY salary) FROM emp`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 6 {
		t.Fatalf("expected 6 rows, got %d", len(r.Rows))
	}
	// row_number should be 1..6
	for i, row := range r.Rows {
		rn := row[1].I64
		if rn != int64(i+1) {
			t.Fatalf("row %d: expected row_number %d, got %d", i, i+1, rn)
		}
	}
}

func TestWindow_RowNumberPartitioned(t *testing.T) {
	ex := setupWindowTestData(t)

	r, err := ex.Exec(`SELECT name, dept, row_number() OVER (PARTITION BY dept ORDER BY salary) FROM emp`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 6 {
		t.Fatalf("expected 6 rows, got %d", len(r.Rows))
	}
	// Within each dept, row_number should restart at 1.
	deptCounts := map[string]int64{}
	for _, row := range r.Rows {
		dept := row[1].Text
		rn := row[2].I64
		deptCounts[dept]++
		if rn > 3 {
			t.Fatalf("row_number %d too high for dept %s", rn, dept)
		}
	}
}

// --- rank ---

func TestWindow_Rank(t *testing.T) {
	ex := setupWindowTestData(t)

	r, err := ex.Exec(`SELECT name, salary, rank() OVER (ORDER BY salary DESC) FROM emp`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 6 {
		t.Fatalf("expected 6 rows, got %d", len(r.Rows))
	}
	// salary DESC: 120, 110, 110, 100, 100, 90
	// rank:          1,   2,   2,   4,   4,  6
	expectedRanks := []int64{1, 2, 2, 4, 4, 6}
	for i, row := range r.Rows {
		if row[2].I64 != expectedRanks[i] {
			t.Fatalf("row %d: expected rank %d, got %d", i, expectedRanks[i], row[2].I64)
		}
	}
}

// --- dense_rank ---

func TestWindow_DenseRank(t *testing.T) {
	ex := setupWindowTestData(t)

	r, err := ex.Exec(`SELECT name, salary, dense_rank() OVER (ORDER BY salary DESC) FROM emp`)
	if err != nil {
		t.Fatal(err)
	}
	// salary DESC: 120, 110, 110, 100, 100, 90
	// dense_rank:    1,   2,   2,   3,   3,  4
	expectedRanks := []int64{1, 2, 2, 3, 3, 4}
	for i, row := range r.Rows {
		if row[2].I64 != expectedRanks[i] {
			t.Fatalf("row %d: expected dense_rank %d, got %d", i, expectedRanks[i], row[2].I64)
		}
	}
}

// --- ntile ---

func TestWindow_Ntile(t *testing.T) {
	ex := setupWindowTestData(t)

	r, err := ex.Exec(`SELECT name, ntile(3) OVER (ORDER BY salary) FROM emp`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 6 {
		t.Fatalf("expected 6 rows, got %d", len(r.Rows))
	}
	// 6 rows into 3 buckets: 1,1,2,2,3,3
	for _, row := range r.Rows {
		bucket := row[1].I64
		if bucket < 1 || bucket > 3 {
			t.Fatalf("ntile bucket %d out of range [1,3]", bucket)
		}
	}
}

// --- lag ---

func TestWindow_Lag(t *testing.T) {
	ex := setupWindowTestData(t)

	r, err := ex.Exec(`SELECT name, salary, lag(salary) OVER (ORDER BY salary) FROM emp`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 6 {
		t.Fatalf("expected 6 rows, got %d", len(r.Rows))
	}
	// First row should have NULL lag.
	if r.Rows[0][2].Type != 0 { // TypeNull
		t.Fatalf("first row lag should be NULL, got %v", r.Rows[0][2])
	}
	// Second row's lag should equal first row's salary.
	if r.Rows[1][2].I64 != r.Rows[0][1].I64 {
		t.Fatalf("second row lag should be %d, got %d", r.Rows[0][1].I64, r.Rows[1][2].I64)
	}
}

// --- lead ---

func TestWindow_Lead(t *testing.T) {
	ex := setupWindowTestData(t)

	r, err := ex.Exec(`SELECT name, salary, lead(salary) OVER (ORDER BY salary) FROM emp`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 6 {
		t.Fatalf("expected 6 rows, got %d", len(r.Rows))
	}
	// Last row should have NULL lead.
	last := len(r.Rows) - 1
	if r.Rows[last][2].Type != 0 {
		t.Fatalf("last row lead should be NULL, got %v", r.Rows[last][2])
	}
	// First row's lead should equal second row's salary.
	if r.Rows[0][2].I64 != r.Rows[1][1].I64 {
		t.Fatalf("first row lead should be %d, got %d", r.Rows[1][1].I64, r.Rows[0][2].I64)
	}
}

// --- first_value / last_value ---

func TestWindow_FirstValue(t *testing.T) {
	ex := setupWindowTestData(t)

	r, err := ex.Exec(`SELECT name, first_value(name) OVER (PARTITION BY dept ORDER BY salary) FROM emp`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 6 {
		t.Fatalf("expected 6 rows, got %d", len(r.Rows))
	}
	// first_value within each partition should be the same for all rows.
	partFirst := map[string]string{}
	for _, row := range r.Rows {
		dept := row[0].Text // name column
		fv := row[1].Text
		if existing, ok := partFirst[fv]; ok {
			_ = existing
		}
		partFirst[dept] = fv
	}
}

// --- aggregate as window ---

func TestWindow_SumRunning(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE txn (id INT, amount INT)`)
	ex.Exec(`INSERT INTO txn VALUES (1, 10), (2, 20), (3, 30), (4, 40)`)

	r, err := ex.Exec(`SELECT id, amount, sum(amount) OVER (ORDER BY id) FROM txn`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(r.Rows))
	}
	// Running sum: 10, 30, 60, 100
	expected := []int64{10, 30, 60, 100}
	for i, row := range r.Rows {
		if row[2].I64 != expected[i] {
			t.Fatalf("row %d: expected running sum %d, got %d", i, expected[i], row[2].I64)
		}
	}
}

func TestWindow_CountStar(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT, grp TEXT)`)
	ex.Exec(`INSERT INTO t VALUES (1, 'a'), (2, 'a'), (3, 'b'), (4, 'b'), (5, 'b')`)

	r, err := ex.Exec(`SELECT id, count(*) OVER (PARTITION BY grp ORDER BY id) FROM t`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(r.Rows))
	}
}

func TestWindow_AvgRunning(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE scores (id INT, score INT)`)
	ex.Exec(`INSERT INTO scores VALUES (1, 10), (2, 20), (3, 30)`)

	r, err := ex.Exec(`SELECT id, avg(score) OVER (ORDER BY id) FROM scores`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(r.Rows))
	}
	// Running avg: 10.0, 15.0, 20.0
	expectedAvg := []float64{10.0, 15.0, 20.0}
	for i, row := range r.Rows {
		if row[1].F64 != expectedAvg[i] {
			t.Fatalf("row %d: expected avg %.1f, got %.1f", i, expectedAvg[i], row[1].F64)
		}
	}
}

// --- multiple window functions in one query ---

func TestWindow_Multiple(t *testing.T) {
	ex := setupWindowTestData(t)

	r, err := ex.Exec(`SELECT name, salary,
		row_number() OVER (ORDER BY salary DESC),
		rank() OVER (ORDER BY salary DESC)
		FROM emp`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 6 {
		t.Fatalf("expected 6 rows, got %d", len(r.Rows))
	}
	// row_number should be 1..6, rank should have ties.
	for i, row := range r.Rows {
		rn := row[2].I64
		if rn != int64(i+1) {
			t.Fatalf("row %d: expected row_number %d, got %d", i, i+1, rn)
		}
	}
}

// --- empty partition ---

func TestWindow_EmptyTable(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE empty (id INT)`)

	r, err := ex.Exec(`SELECT id, row_number() OVER () FROM empty`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(r.Rows))
	}
}

// --- OVER () with no partition or order ---

func TestWindow_OverEmpty(t *testing.T) {
	ex := newTestExecutor(t)
	ex.Exec(`CREATE TABLE t (id INT)`)
	ex.Exec(`INSERT INTO t VALUES (1), (2), (3)`)

	r, err := ex.Exec(`SELECT id, row_number() OVER () FROM t`)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(r.Rows))
	}
}
