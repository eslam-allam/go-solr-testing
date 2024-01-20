package main

import (
	"log"
	"time"
	"github.com/eslam-allam/go-solr-testing/solr"
)

func main() {
	start := time.Now()

	solrCursor := solr.Cursor("localhost", 8983, "adri_documents", 10, "id asc")

	i := 1
	for doc, done, err := solrCursor.Next(); !done; doc, done, err = solrCursor.Next() {

		if err != nil {
			log.Fatal(err)
		}

		title, ok := doc["original_dc_title"]

		if !ok {
			log.Fatalf("Could not fetch title of document #%d", i)
		}
		log.Printf("%d. %s", i, title)
		i++
	}

	log.Printf("Total execution time: %s", time.Since(start))
}
