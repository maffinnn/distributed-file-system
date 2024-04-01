package rpc

type Message struct {
	Header Header
	Body   interface{}
}

type Header struct {
	ServiceMethod string // format "Service.Method" will be casted to string
	Seq           uint64 // sequence number chosen by client
	Error         string
}

type Codec interface {
	Decode([]byte, *Message) error
	Encode(*Header, interface{}) ([]byte, error)
}

type NewCodecFunc func() Codec

const (
	LabType Type = "application/lab"
)

const (
	DefaultCodecType = LabType
	MaxBufferSize    = 1024 * 50
)

type Type string

var NewCodecFuncMap = map[Type]NewCodecFunc{
	LabType: NewLabCodec,
}
