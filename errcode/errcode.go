package errcode

const (
	Errcode_ok = int16(0)
	Errcode_version_mismatch
	Errcode_record_exist
	Errcode_record_notexist
	Errcode_record_unchange
	Errcode_cas_not_equal
	Errcode_timeout
	Errcode_retry
	Errcode_error
)

type Error *error

type error struct {
	Code int16
	Desc string
}

func GetErrorDesc(e Error) string {
	if nil == e {
		return "no error"
	} else {
		switch e.Code {
		case Errcode_version_mismatch:
			return "version mismatch"
		case Errcode_record_exist:
			return "record is exist already"
		case Errcode_record_notexist:
			return "record is not exist"
		case Errcode_record_unchange:
			return "record is unchange"
		case Errcode_cas_not_equal:
			return "cas old value is not equal"
		case Errcode_retry:
			if "" == e.Desc {
				return "please retry later"
			} else {
				return e.Desc
			}
		default:
			return e.Desc
		}
	}
}

func New(code int16, desc string) Error {
	return &error{
		Code: code,
		Desc: desc,
	}
}
