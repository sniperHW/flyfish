package flyfish

import (
	"fmt"
	//pb "github.com/golang/protobuf/proto"
	"github.com/sniperHW/flyfish/proto"
	"strconv"
)

var pgsqlByteToString = []string{
	"\\000",
	"\\001",
	"\\002",
	"\\003",
	"\\004",
	"\\005",
	"\\006",
	"\\007",
	"\\010",
	"\\011",
	"\\012",
	"\\013",
	"\\014",
	"\\015",
	"\\016",
	"\\017",
	"\\020",
	"\\021",
	"\\022",
	"\\023",
	"\\024",
	"\\025",
	"\\026",
	"\\027",
	"\\030",
	"\\031",
	"\\032",
	"\\033",
	"\\034",
	"\\035",
	"\\036",
	"\\037",
	"\\040",
	"\\041",
	"\\042",
	"\\043",
	"\\044",
	"\\045",
	"\\046",
	"\\047",
	"\\050",
	"\\051",
	"\\052",
	"\\053",
	"\\054",
	"\\055",
	"\\056",
	"\\057",
	"\\060",
	"\\061",
	"\\062",
	"\\063",
	"\\064",
	"\\065",
	"\\066",
	"\\067",
	"\\070",
	"\\071",
	"\\072",
	"\\073",
	"\\074",
	"\\075",
	"\\076",
	"\\077",
	"\\100",
	"\\101",
	"\\102",
	"\\103",
	"\\104",
	"\\105",
	"\\106",
	"\\107",
	"\\110",
	"\\111",
	"\\112",
	"\\113",
	"\\114",
	"\\115",
	"\\116",
	"\\117",
	"\\120",
	"\\121",
	"\\122",
	"\\123",
	"\\124",
	"\\125",
	"\\126",
	"\\127",
	"\\130",
	"\\131",
	"\\132",
	"\\133",
	"\\134",
	"\\135",
	"\\136",
	"\\137",
	"\\140",
	"\\141",
	"\\142",
	"\\143",
	"\\144",
	"\\145",
	"\\146",
	"\\147",
	"\\150",
	"\\151",
	"\\152",
	"\\153",
	"\\154",
	"\\155",
	"\\156",
	"\\157",
	"\\160",
	"\\161",
	"\\162",
	"\\163",
	"\\164",
	"\\165",
	"\\166",
	"\\167",
	"\\170",
	"\\171",
	"\\172",
	"\\173",
	"\\174",
	"\\175",
	"\\176",
	"\\177",
	"\\200",
	"\\201",
	"\\202",
	"\\203",
	"\\204",
	"\\205",
	"\\206",
	"\\207",
	"\\210",
	"\\211",
	"\\212",
	"\\213",
	"\\214",
	"\\215",
	"\\216",
	"\\217",
	"\\220",
	"\\221",
	"\\222",
	"\\223",
	"\\224",
	"\\225",
	"\\226",
	"\\227",
	"\\230",
	"\\231",
	"\\232",
	"\\233",
	"\\234",
	"\\235",
	"\\236",
	"\\237",
	"\\240",
	"\\241",
	"\\242",
	"\\243",
	"\\244",
	"\\245",
	"\\246",
	"\\247",
	"\\250",
	"\\251",
	"\\252",
	"\\253",
	"\\254",
	"\\255",
	"\\256",
	"\\257",
	"\\260",
	"\\261",
	"\\262",
	"\\263",
	"\\264",
	"\\265",
	"\\266",
	"\\267",
	"\\270",
	"\\271",
	"\\272",
	"\\273",
	"\\274",
	"\\275",
	"\\276",
	"\\277",
	"\\300",
	"\\301",
	"\\302",
	"\\303",
	"\\304",
	"\\305",
	"\\306",
	"\\307",
	"\\310",
	"\\311",
	"\\312",
	"\\313",
	"\\314",
	"\\315",
	"\\316",
	"\\317",
	"\\320",
	"\\321",
	"\\322",
	"\\323",
	"\\324",
	"\\325",
	"\\326",
	"\\327",
	"\\330",
	"\\331",
	"\\332",
	"\\333",
	"\\334",
	"\\335",
	"\\336",
	"\\337",
	"\\340",
	"\\341",
	"\\342",
	"\\343",
	"\\344",
	"\\345",
	"\\346",
	"\\347",
	"\\350",
	"\\351",
	"\\352",
	"\\353",
	"\\354",
	"\\355",
	"\\356",
	"\\357",
	"\\360",
	"\\361",
	"\\362",
	"\\363",
	"\\364",
	"\\365",
	"\\366",
	"\\367",
	"\\370",
	"\\371",
	"\\372",
	"\\373",
	"\\374",
	"\\375",
	"\\376",
	"\\377",
}

var pgsqlSuffix = []byte{'\047', '\072', '\072', '\142', '\171', '\164', '\145', '\141'}

//'\075'::bytea
func pgsqlBinaryToPgsqlStr(s *str, bytes []byte) {
	s.append("'")
	for _, v := range bytes {
		s.append(pgsqlByteToString[int(v)])
	}
	s.append("'::bytea")
}

func fieldToString(s *str, field *proto.Field) {

	tt := field.GetType()

	switch tt {
	case proto.ValueType_string:
		s.append(fmt.Sprintf("'%s'", field.GetString()))
	case proto.ValueType_float:
		s.append(fmt.Sprintf("%f", field.GetFloat()))
	case proto.ValueType_int:
		s.append(strconv.FormatInt(field.GetInt(), 10))
	case proto.ValueType_uint:
		s.append(strconv.FormatUint(field.GetUint(), 10))
	case proto.ValueType_blob:
		pgsqlBinaryToPgsqlStr(s, field.GetBlob())
	default:
		panic("invaild value type")
	}
}

func buildInsertString(s *str, r *proto.Record, meta *table_meta) {
	s.append(meta.insertPrefix).append("'").append(r.GetKey()).append("',") //add __key__
	fieldToString(s, r.Fields[0])                                           //add __version__
	s.append(",")

	//add other fileds
	for i := 1; i < len(r.Fields); i++ {
		fieldToString(s, r.Fields[i])
		if i != len(r.Fields)-1 {
			s.append(",")
		}
	}

	s.append(");")
}

func buildUpdateString(s *str, r *proto.Record, meta *table_meta) {
	s.append("update ").append(r.GetTable()).append(" set ")

	for i, v := range r.Fields {
		if i == 0 {
			s.append(v.GetName()).append("=")
			fieldToString(s, v)
		} else {
			s.append(",").append(v.GetName()).append("=")
			fieldToString(s, v)
		}
	}

	s.append(" where __key__ = '").append(r.GetKey()).append("';")
}

func buildDeleteString(s *str, r *proto.Record) {
	s.append("delete from ").append(r.GetTable()).append(" where __key__ = '").append(r.GetKey()).append("';")
}
