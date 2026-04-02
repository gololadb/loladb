// gen_data generates INSERT statements for the simplified pagila schema.
// Output is valid SQL for both PostgreSQL and LolaDB.
//
// Usage: go run gen_data.go > data.sql
package main

import (
	"fmt"
	"math/rand"
)

var firstNames = []string{
	"PENELOPE", "NICK", "ED", "JENNIFER", "JOHNNY", "BETTE", "GRACE",
	"MATTHEW", "JOE", "CHRISTIAN", "ZERO", "KARL", "UMA", "VIVIEN",
	"CUBA", "FRED", "HELEN", "DAN", "BOB", "LUCILLE", "KIRSTEN",
	"ELVIS", "SANDRA", "CAMERON", "KEVIN", "RIP", "JULIA", "WOODY",
	"ALEC", "RENEE", "SISSY", "TIM", "ALLEN", "JUDY", "BURT",
	"VAL", "TOM", "TRACEY", "HELEN", "BEN", "GOLDIE", "SEAN",
	"GARY", "DUSTIN", "HENRY", "GENE", "MORGAN", "EMILY", "MARY",
	"ANGELA", "RUSSELL", "JAYNE", "MINNIE", "GREG", "SPENCER",
	"KENNETH", "MENA", "JAMES", "ROCK", "HUMPHREY",
}

var lastNames = []string{
	"GUINESS", "WAHLBERG", "CHASE", "DAVIS", "LOLLOBRIGIDA", "NICHOLSON",
	"MOSTEL", "JOHANSSON", "SWANK", "GABLE", "CAGE", "BERRY", "WOOD",
	"BERGEN", "OLIVIER", "COSTNER", "VOIGHT", "TORN", "FAWCETT",
	"TEMPLE", "NOLTE", "SINATRA", "WILLIS", "PECK", "KILMER",
	"MONROE", "STREEP", "DENCH", "TANDY", "KEITEL", "JACKMAN",
	"WINSLET", "HACKMAN", "PALTROW", "BENING", "HOPKINS", "MCKELLEN",
	"DEGENERES", "GARLAND", "BRODY", "DEPP", "AKROYD", "DREYFUSS",
	"BIRCH", "BAILEY", "CRONYN", "DUKAKIS", "NEESON", "TRACY",
	"HARRIS", "CRUZ", "DAMON", "JOLIE", "PESCI", "PRESLEY",
	"ZELLWEGER", "BACALL", "WAYNE", "WEST", "SILVERSTONE",
}

var categories = []string{
	"Action", "Animation", "Children", "Classics", "Comedy",
	"Documentary", "Drama", "Family", "Foreign", "Games",
	"Horror", "Music", "New", "Sci-Fi", "Sports", "Travel",
}

var ratings = []string{"G", "PG", "PG-13", "R", "NC-17"}

var filmTitleWords = []string{
	"ACADEMY", "ACE", "ADAPTATION", "AFFAIR", "AFRICAN", "AGENT",
	"AIRPLANE", "AIRPORT", "ALABAMA", "ALADDIN", "ALAMO", "ALASKA",
	"ALI", "ALICE", "ALIEN", "ALLEY", "ALONE", "ALTER", "AMADEUS",
	"AMELIE", "AMERICAN", "AMISTAD", "ANACONDA", "ANALYZE", "ANGELS",
	"ANNIE", "ANONYMOUS", "APACHE", "APOCALYPSE", "APOLLO", "ARENA",
	"ARMAGEDDON", "ARMY", "ARSENIC", "ATLANTIS", "ATTACKS", "AUTUMN",
	"BABY", "BACKLASH", "BADMAN", "BAKED", "BALLOON", "BANG",
	"BARBARELLA", "BAREFOOT", "BASIC", "BEACH", "BEAR", "BEAST",
	"BEAUTY", "BED", "BEDAZZLED", "BEETHOVEN", "BEHAVIOR", "BENEATH",
	"BETRAYED", "BEVERLY", "BIKINI", "BILKO", "BILLION", "BIRD",
	"BLADE", "BLANKET", "BLINDNESS", "BLOOD", "BLUES", "BOILED",
	"BONNIE", "BOOGIE", "BORN", "BORROWERS", "BOULEVARD", "BOUND",
	"BOWFINGER", "BRANNIGAN", "BRAVEHEART", "BREAKFAST", "BREAKING",
	"BRIDE", "BRIGHT", "BRINGING", "BROOKLYN", "BROTHERHOOD", "BUBBLE",
	"BUCKET", "BUGSY", "BULL", "BULWORTH", "BUNCH", "BUTCH",
	"BUTTERFLY", "CABIN", "CALENDAR", "CALIFORNIA", "CAMELOT",
	"CAMPUS", "CANDIDATE", "CANDLES", "CANYON", "CAPER", "CARIBBEAN",
}

func main() {
	rng := rand.New(rand.NewSource(42))

	// Actors: 200
	numActors := 200
	fmt.Println("-- actors")
	for i := 1; i <= numActors; i++ {
		fn := firstNames[rng.Intn(len(firstNames))]
		ln := lastNames[rng.Intn(len(lastNames))]
		fmt.Printf("INSERT INTO actor VALUES (%d, '%s', '%s');\n", i, fn, ln)
	}

	// Categories: 16
	fmt.Println("\n-- categories")
	for i, name := range categories {
		fmt.Printf("INSERT INTO category VALUES (%d, '%s');\n", i+1, name)
	}

	// Films: 1000
	numFilms := 1000
	fmt.Println("\n-- films")
	for i := 1; i <= numFilms; i++ {
		w1 := filmTitleWords[rng.Intn(len(filmTitleWords))]
		w2 := filmTitleWords[rng.Intn(len(filmTitleWords))]
		title := fmt.Sprintf("%s %s", w1, w2)
		desc := fmt.Sprintf("A story about %s and %s", w1, w2)
		year := 1990 + rng.Intn(25)
		duration := 3 + rng.Intn(5)
		rate := 1 + rng.Intn(5)
		length := 60 + rng.Intn(120)
		rating := ratings[rng.Intn(len(ratings))]
		fmt.Printf("INSERT INTO film VALUES (%d, '%s', '%s', %d, %d, %d, %d, '%s');\n",
			i, title, desc, year, duration, rate, length, rating)
	}

	// Film-actor: ~5 actors per film = 5000 rows
	fmt.Println("\n-- film_actor")
	for filmID := 1; filmID <= numFilms; filmID++ {
		nActors := 3 + rng.Intn(6) // 3-8 actors per film
		used := map[int]bool{}
		for j := 0; j < nActors; j++ {
			actorID := 1 + rng.Intn(numActors)
			if used[actorID] {
				continue
			}
			used[actorID] = true
			fmt.Printf("INSERT INTO film_actor VALUES (%d, %d);\n", actorID, filmID)
		}
	}

	// Film-category: 1 category per film
	fmt.Println("\n-- film_category")
	for filmID := 1; filmID <= numFilms; filmID++ {
		catID := 1 + rng.Intn(len(categories))
		fmt.Printf("INSERT INTO film_category VALUES (%d, %d);\n", filmID, catID)
	}

	// Customers: 599
	numCustomers := 599
	fmt.Println("\n-- customers")
	for i := 1; i <= numCustomers; i++ {
		fn := firstNames[rng.Intn(len(firstNames))]
		ln := lastNames[rng.Intn(len(lastNames))]
		email := fmt.Sprintf("%s.%s@example.com", fn, ln)
		active := 1
		if rng.Float64() < 0.05 {
			active = 0
		}
		fmt.Printf("INSERT INTO customer VALUES (%d, '%s', '%s', '%s', %d);\n",
			i, fn, ln, email, active)
	}

	// Stores: 2
	fmt.Println("\n-- stores")
	fmt.Println("INSERT INTO store VALUES (1, 1, '47 MySakila Drive');")
	fmt.Println("INSERT INTO store VALUES (2, 2, '28 MySQL Boulevard');")

	// Inventory: ~4 copies per film per store = ~8000
	numInventory := 0
	fmt.Println("\n-- inventory")
	for filmID := 1; filmID <= numFilms; filmID++ {
		for storeID := 1; storeID <= 2; storeID++ {
			copies := 2 + rng.Intn(4) // 2-5 copies
			for c := 0; c < copies; c++ {
				numInventory++
				fmt.Printf("INSERT INTO inventory VALUES (%d, %d, %d);\n",
					numInventory, filmID, storeID)
			}
		}
	}

	// Rentals: ~16000
	numRentals := 16000
	fmt.Println("\n-- rentals")
	for i := 1; i <= numRentals; i++ {
		invID := 1 + rng.Intn(numInventory)
		custID := 1 + rng.Intn(numCustomers)
		staffID := 1 + rng.Intn(2)
		status := "returned"
		if rng.Float64() < 0.1 {
			status = "active"
		}
		fmt.Printf("INSERT INTO rental VALUES (%d, %d, %d, %d, '%s');\n",
			i, invID, custID, staffID, status)
	}

	// Payments: ~16000 (one per rental)
	fmt.Println("\n-- payments")
	for i := 1; i <= numRentals; i++ {
		custID := 1 + rng.Intn(numCustomers)
		staffID := 1 + rng.Intn(2)
		amount := 1 + rng.Intn(10)
		fmt.Printf("INSERT INTO payment VALUES (%d, %d, %d, %d, %d);\n",
			i, custID, staffID, i, amount)
	}
}
