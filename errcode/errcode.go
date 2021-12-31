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
	Errcode_end
)

var errDesc []string = []string{
	"no error",
	"version mismatch",
	"record is exist already",
	"record is not exist",
	"record is unchange",
	"cas old value is not equal",
	"timeout",
	"please retry later",
	"other error",
	"not leader",
	"gate busy",
	"slot is transfering",
	"route info stale",
}

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
	} else if e.Desc != "" {
		return e.Desc
	} else if e.Code >= Errcode_ok && e.Code < Errcode_end {
		return errDesc[e.Code]
	} else {
		return "invaild error code"
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
