package solr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
)

type solrCursor struct {
	solrUrl       string
	currentCursor string
	prevCursor    string
	sort          string
	docBuffer     []solrDoc
	bufferSize    int
	done          bool
}


func (s *solrCursor) Next() (doc solrDoc, done bool, err error) {
	base, err := url.Parse(s.solrUrl)
	if err != nil {
		return nil, false, err
	}

	base.Path += "/select"

	params := url.Values{}

	params.Add("q", "*:*")
	params.Add("cursorMark", s.currentCursor)
	params.Add("rows", fmt.Sprint(s.bufferSize))
	params.Add("wt", "json")
	params.Add("sort", s.sort)

	base.RawQuery = params.Encode()

	if len(s.docBuffer) == 0 {

		err := fetchSolrBuffer(base, s)
		if err != nil {
			return nil, false, err
		}
	}

  if s.done {
    return nil, true, nil
  }

	var nextDoc solrDoc

	nextDoc, s.docBuffer = s.docBuffer[0], s.docBuffer[1:]

	return nextDoc, false, nil
}

func fetchSolrBuffer(base *url.URL, s *solrCursor) error {

	response, err := http.Get(base.String())

	if err != nil {

		return err
	}

	if response.StatusCode != 200 {

    body, err := io.ReadAll(io.Reader(response.Body))

    if err != nil {
      return err
    }
		return fmt.Errorf("recieved failure status code from solr: %s", string(body))
	}

	var solrResponse map[string]interface{}

	err = json.NewDecoder(response.Body).Decode(&solrResponse)

	if err != nil {
		return err
	}

	resp, ok := solrResponse["response"]

	if !ok {
		return errors.New("could not retrieve solr response")
	}

	respMap, ok := resp.(map[string]interface{})

	if !ok {
		return errors.New("solr response is not a map")
	}

	docs, ok := respMap["docs"]

	if !ok {
		return errors.New("couldn't find docs in solr response")
	}

  rawDocs, ok := docs.([]interface{})

	if !ok {
    return fmt.Errorf("solr doc response is not a list of elements. Real type %s", reflect.TypeOf(docs))
	}

  s.docBuffer, err = convertSolrDocs(rawDocs)

  if err != nil {
    return err
  }

	s.currentCursor, ok = solrResponse["nextCursorMark"].(string)

	if !ok {
		return errors.New("could not grab next cursor mark from solr response")
	}

	if s.prevCursor == s.currentCursor {
		s.done = true
	}

  s.prevCursor = s.currentCursor
	return nil
}

func convertSolrDocs(docsRaw []interface{}) ([]solrDoc, error)  {
  convertedDocs := make([]solrDoc, len(docsRaw))

  for i, val := range docsRaw {
    doc, ok := val.(map[string]interface{})
    if !ok {
      return nil, fmt.Errorf("failed to convert raw docs to solr docs. %s", reflect.TypeOf(val))
    }
    convertedDocs[i] = doc
  }

  return convertedDocs, nil

}

type solrDoc map[string]interface{}

func Cursor(host string, port int, collection string, bufferSize int, sort string) solrCursor {
	return solrCursor{
		solrUrl:       fmt.Sprintf("http://%s:%d/solr/%s", host, port, collection),
		currentCursor: "*",
		prevCursor:    "*",
		docBuffer:     make([]solrDoc, 0),
		bufferSize:    bufferSize,
    sort: sort,
	}
}
