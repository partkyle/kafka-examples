package sarama

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
)

// CompressionCodec represents the various compression codecs recognized by Kafka in messages.
type CompressionCodec int8

// only the last two bits are really used
const compressionCodecMask int8 = 0x03

const (
	CompressionNone   CompressionCodec = 0
	CompressionGZIP   CompressionCodec = 1
	CompressionSnappy CompressionCodec = 2
)

// The spec just says: "This is a version id used to allow backwards compatible evolution of the message
// binary format." but it doesn't say what the current value is, so presumably 0...
const messageFormat int8 = 0

type Message struct {
	Codec CompressionCodec // codec used to compress the message contents
	Key   []byte           // the message key, may be nil
	Value []byte           // the message contents
	Set   *MessageSet      // the message set a message might wrap

	compressedCache []byte
}

func (m *Message) encode(pe packetEncoder) error {
	pe.push(&crc32Field{})

	pe.putInt8(messageFormat)

	attributes := int8(m.Codec) & compressionCodecMask
	pe.putInt8(attributes)

	err := pe.putBytes(m.Key)
	if err != nil {
		return err
	}

	var payload []byte

	if m.compressedCache != nil {
		payload = m.compressedCache
		m.compressedCache = nil
	} else {
		switch m.Codec {
		case CompressionNone:
			payload = m.Value
		case CompressionGZIP:
			var buf bytes.Buffer
			writer := gzip.NewWriter(&buf)
			writer.Write(m.Value)
			writer.Close()
			m.compressedCache = buf.Bytes()
			payload = m.compressedCache
		case CompressionSnappy:
			tmp, err := SnappyEncode(m.Value)
			if err != nil {
				return err
			}
			m.compressedCache = tmp
			payload = m.compressedCache
		default:
			return EncodingError
		}
	}

	err = pe.putBytes(payload)
	if err != nil {
		return err
	}

	return pe.pop()
}

func (m *Message) decode(pd packetDecoder) (err error) {
	err = pd.push(&crc32Field{})
	if err != nil {
		return err
	}

	format, err := pd.getInt8()
	if err != nil {
		return err
	}
	if format != messageFormat {
		return DecodingError{Info: "Unexpected messageFormat"}
	}

	attribute, err := pd.getInt8()
	if err != nil {
		return err
	}
	m.Codec = CompressionCodec(attribute & compressionCodecMask)

	m.Key, err = pd.getBytes()
	if err != nil {
		return err
	}

	m.Value, err = pd.getBytes()
	if err != nil {
		return err
	}

	switch m.Codec {
	case CompressionNone:
		// nothing to do
	case CompressionGZIP:
		if m.Value == nil {
			return DecodingError{Info: "GZIP compression specified, but no data to uncompress"}
		}
		reader, err := gzip.NewReader(bytes.NewReader(m.Value))
		if err != nil {
			return err
		}
		if m.Value, err = ioutil.ReadAll(reader); err != nil {
			return err
		}
		return m.decodeSet()
	case CompressionSnappy:
		if m.Value == nil {
			return DecodingError{Info: "Snappy compression specified, but no data to uncompress"}
		}
		if m.Value, err = SnappyDecode(m.Value); err != nil {
			return err
		}
		return m.decodeSet()
	default:
		return DecodingError{Info: "Invalid compression specified"}
	}

	err = pd.pop()
	if err != nil {
		return err
	}

	return nil
}

// decodes a message set from a previousy encoded bulk-message
func (m *Message) decodeSet() (err error) {
	pd := realDecoder{raw: m.Value}
	m.Set = &MessageSet{}
	return m.Set.decode(&pd)
}
