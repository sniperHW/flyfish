package errcode

const (
	Errcode_ok = int16(iota)
	Errcode_version_mismatch
	Errcode_record_exist
	Errcode_record_notexist
	Errcode_record_unchange
	Errcode_cas_not_equal
	Errcode_timeout
	Errcode_retry
	Errcode_error
	Errcode_not_leader
	Errcode_gate_busy
	Errcode_slot_transfering
	Errcode_route_info_stale
)

type Error *error

type error struct {
	Code int16
	Desc string
}

func GetCode(e Error) int16 {
	if nil == e {
		return Errcode_ok
	} else {
		return e.Code
	}
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
		case Errcode_not_leader:
			return "not leader"
		case Errcode_gate_busy:
			return "gate busy"
		case Errcode_retry:
			if "" == e.Desc {
				return "please retry later"
			} else {
				return e.Desc
			}
		case Errcode_slot_transfering:
			return "slot transfering"
		case Errcode_route_info_stale:
			return "Errcode_route_info_stale"
		default:
			return e.Desc
		}
	}
}

func New(code int16, desc ...string) Error {
	err := &error{
		Code: code,
	}
	if len(desc) > 0 {
		err.Desc = desc[0]
	}
	return err
}
