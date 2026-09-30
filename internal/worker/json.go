package worker

import (
	"encoding/json"
	"io"
)

func jsonNewDecoder(reader io.Reader) *json.Decoder {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	return decoder
}

func jsonNewEncoder(writer io.Writer) *json.Encoder {
	return json.NewEncoder(writer)
}
