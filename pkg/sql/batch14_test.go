package sql

import (
	"math"
	"strings"
	"testing"
)

// --- Geometric column types ---

func TestGeometricColumnTypes(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE TABLE shapes (id INT, pt POINT, bx BOX, cir CIRCLE, seg LSEG, pg POLYGON, pa PATH)")
	mustExec(t, ex, "INSERT INTO shapes VALUES (1, '(1,2)', '(3,4),(0,0)', '<(5,5),3>', '[(0,0),(1,1)]', '((0,0),(1,0),(1,1),(0,1))', '[(0,0),(1,1),(2,0)]')")

	r := mustExecR(t, ex, "SELECT pt, bx, cir FROM shapes WHERE id = 1")
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Text != "(1,2)" {
		t.Fatalf("point expected '(1,2)', got %q", r.Rows[0][0].Text)
	}
	if r.Rows[0][1].Text != "(3,4),(0,0)" {
		t.Fatalf("box expected '(3,4),(0,0)', got %q", r.Rows[0][1].Text)
	}
	if r.Rows[0][2].Text != "<(5,5),3>" {
		t.Fatalf("circle expected '<(5,5),3>', got %q", r.Rows[0][2].Text)
	}
}

// --- Constructor functions ---

func TestPointConstructor(t *testing.T) {
	ex := newTestExecutor(t)
	r := mustExecR(t, ex, "SELECT point(3, 4)")
	if r.Rows[0][0].Text != "(3,4)" {
		t.Fatalf("point(3,4) expected '(3,4)', got %q", r.Rows[0][0].Text)
	}
}

func TestBoxConstructor(t *testing.T) {
	ex := newTestExecutor(t)
	r := mustExecR(t, ex, "SELECT box(point(1,2), point(3,4))")
	if r.Rows[0][0].Text != "(1,2),(3,4)" {
		t.Fatalf("box expected '(1,2),(3,4)', got %q", r.Rows[0][0].Text)
	}
}

func TestCircleConstructor(t *testing.T) {
	ex := newTestExecutor(t)
	r := mustExecR(t, ex, "SELECT circle(point(0,0), 5)")
	if r.Rows[0][0].Text != "<(0,0),5>" {
		t.Fatalf("circle expected '<(0,0),5>', got %q", r.Rows[0][0].Text)
	}
}

func TestLsegConstructor(t *testing.T) {
	ex := newTestExecutor(t)
	r := mustExecR(t, ex, "SELECT lseg(point(0,0), point(1,1))")
	if r.Rows[0][0].Text != "[(0,0),(1,1)]" {
		t.Fatalf("lseg expected '[(0,0),(1,1)]', got %q", r.Rows[0][0].Text)
	}
}

// --- Accessor / measurement functions ---

func TestAreaCircle(t *testing.T) {
	ex := newTestExecutor(t)
	r := mustExecR(t, ex, "SELECT area('<(0,0),5>')")
	expected := math.Pi * 25
	got := r.Rows[0][0].F64
	if math.Abs(got-expected) > 0.001 {
		t.Fatalf("area(circle r=5) expected %f, got %f", expected, got)
	}
}

func TestAreaBox(t *testing.T) {
	ex := newTestExecutor(t)
	r := mustExecR(t, ex, "SELECT area('(3,4),(0,0)')")
	got := r.Rows[0][0].F64
	if math.Abs(got-12) > 0.001 {
		t.Fatalf("area(box 3x4) expected 12, got %f", got)
	}
}

func TestCenter(t *testing.T) {
	ex := newTestExecutor(t)
	r := mustExecR(t, ex, "SELECT center('<(3,4),10>')")
	if r.Rows[0][0].Text != "(3,4)" {
		t.Fatalf("center(circle) expected '(3,4)', got %q", r.Rows[0][0].Text)
	}

	r = mustExecR(t, ex, "SELECT center('(4,4),(0,0)')")
	if r.Rows[0][0].Text != "(2,2)" {
		t.Fatalf("center(box) expected '(2,2)', got %q", r.Rows[0][0].Text)
	}
}

func TestDiameterRadius(t *testing.T) {
	ex := newTestExecutor(t)
	r := mustExecR(t, ex, "SELECT diameter('<(0,0),7>')")
	if math.Abs(r.Rows[0][0].F64-14) > 0.001 {
		t.Fatalf("diameter expected 14, got %f", r.Rows[0][0].F64)
	}

	r = mustExecR(t, ex, "SELECT radius('<(0,0),7>')")
	if math.Abs(r.Rows[0][0].F64-7) > 0.001 {
		t.Fatalf("radius expected 7, got %f", r.Rows[0][0].F64)
	}
}

func TestHeightWidth(t *testing.T) {
	ex := newTestExecutor(t)
	r := mustExecR(t, ex, "SELECT height('(5,8),(2,3)')")
	if math.Abs(r.Rows[0][0].F64-5) > 0.001 {
		t.Fatalf("height expected 5, got %f", r.Rows[0][0].F64)
	}

	r = mustExecR(t, ex, "SELECT width('(5,8),(2,3)')")
	if math.Abs(r.Rows[0][0].F64-3) > 0.001 {
		t.Fatalf("width expected 3, got %f", r.Rows[0][0].F64)
	}
}

func TestNpoints(t *testing.T) {
	ex := newTestExecutor(t)
	r := mustExecR(t, ex, "SELECT npoints('((0,0),(1,0),(1,1),(0,1))')")
	if r.Rows[0][0].I64 != 4 {
		t.Fatalf("npoints expected 4, got %d", r.Rows[0][0].I64)
	}
}

func TestIsClosedIsOpen(t *testing.T) {
	ex := newTestExecutor(t)
	// Closed path uses '(...)'.
	r := mustExecR(t, ex, "SELECT isclosed('((0,0),(1,0),(1,1))')")
	if !r.Rows[0][0].Bool {
		t.Fatal("isclosed('(...)') expected true")
	}

	r = mustExecR(t, ex, "SELECT isopen('[(0,0),(1,0),(1,1)]')")
	if !r.Rows[0][0].Bool {
		t.Fatal("isopen('[...]') expected true")
	}

	r = mustExecR(t, ex, "SELECT isclosed('[(0,0),(1,0)]')")
	if r.Rows[0][0].Bool {
		t.Fatal("isclosed('[...]') expected false")
	}
}

func TestPclosePopen(t *testing.T) {
	ex := newTestExecutor(t)
	r := mustExecR(t, ex, "SELECT pclose('[(0,0),(1,1)]')")
	if !strings.HasPrefix(r.Rows[0][0].Text, "(") {
		t.Fatalf("pclose expected '(...)', got %q", r.Rows[0][0].Text)
	}

	r = mustExecR(t, ex, "SELECT popen('((0,0),(1,1))')")
	if !strings.HasPrefix(r.Rows[0][0].Text, "[") {
		t.Fatalf("popen expected '[...]', got %q", r.Rows[0][0].Text)
	}
}

// --- Distance operator <-> ---

func TestGeomDistanceOperator(t *testing.T) {
	ex := newTestExecutor(t)
	r := mustExecR(t, ex, "SELECT '(0,0)' <-> '(3,4)'")
	if math.Abs(r.Rows[0][0].F64-5) > 0.001 {
		t.Fatalf("distance (0,0)<->(3,4) expected 5, got %f", r.Rows[0][0].F64)
	}
}

// --- Distance in queries ---

func TestGeomDistanceQuery(t *testing.T) {
	ex := newTestExecutor(t)
	mustExec(t, ex, "CREATE TABLE places (name TEXT, loc POINT)")
	mustExec(t, ex, "INSERT INTO places VALUES ('A', '(0,0)'), ('B', '(3,4)'), ('C', '(10,10)')")

	// Find places within distance 6 of origin.
	r := mustExecR(t, ex, "SELECT name FROM places WHERE loc <-> '(0,0)' < 6 ORDER BY name")
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 places within distance 6, got %d", len(r.Rows))
	}
	if r.Rows[0][0].Text != "A" || r.Rows[1][0].Text != "B" {
		t.Fatalf("expected A and B, got %q and %q", r.Rows[0][0].Text, r.Rows[1][0].Text)
	}
}

// --- bound_box ---

func TestBoundBox(t *testing.T) {
	ex := newTestExecutor(t)
	r := mustExecR(t, ex, "SELECT bound_box('(3,4),(0,0)', '(5,6),(2,1)')")
	// Bounding box should be (5,6),(0,0).
	got := r.Rows[0][0].Text
	if !strings.Contains(got, "5") || !strings.Contains(got, "0") {
		t.Fatalf("bound_box expected to contain corners 5,6 and 0,0, got %q", got)
	}
}
