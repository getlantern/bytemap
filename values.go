package bytemap

type valuesIF interface {
	get(idx int) interface{}
}

type interfaceValues []interface{}

func (iv interfaceValues) get(idx int) interface{} {
	return iv[idx]
}

type floatValues []float64

func (fv floatValues) get(idx int) interface{} {
	return fv[idx]
}
