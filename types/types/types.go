package types

// DataType represents the type of data in a Series
type DataType interface {
	String() string
}

// Primitive data types
type (
	Int64Type   struct{}
	Float64Type struct{}
	StringType  struct{}
	BooleanType struct{}
)

func (Int64Type) String() string   { return "Int64" }
func (Float64Type) String() string { return "Float64" }
func (StringType) String() string  { return "String" }
func (BooleanType) String() string { return "Boolean" }

// Series represents a single column of data
type Series struct {
	Name     string
	DataType DataType
	Data     interface{} // Will hold []int64, []float64, []string, or []bool
	Length   int
}

// NewSeries creates a new Series with the given name and data
func NewSeries(name string, data interface{}) *Series {
	switch d := data.(type) {
	case []int64:
		return &Series{
			Name:     name,
			DataType: Int64Type{},
			Data:     d,
			Length:   len(d),
		}
	case []float64:
		return &Series{
			Name:     name,
			DataType: Float64Type{},
			Data:     d,
			Length:   len(d),
		}
	case []string:
		return &Series{
			Name:     name,
			DataType: StringType{},
			Data:     d,
			Length:   len(d),
		}
	case []bool:
		return &Series{
			Name:     name,
			DataType: BooleanType{},
			Data:     d,
			Length:   len(d),
		}
	default:
		panic("unsupported data type")
	}
}
