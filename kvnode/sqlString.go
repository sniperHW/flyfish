package kvnode

import (
	"github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/util/str"
)

var mysqlByteToString = []string{
	"00",
	"01",
	"02",
	"03",
	"04",
	"05",
	"06",
	"07",
	"08",
	"09",
	"0a",
	"0b",
	"0c",
	"0d",
	"0e",
	"0f",
	"10",
	"11",
	"12",
	"13",
	"14",
	"15",
	"16",
	"17",
	"18",
	"19",
	"1a",
	"1b",
	"1c",
	"1d",
	"1e",
	"1f",
	"20",
	"21",
	"22",
	"23",
	"24",
	"25",
	"26",
	"27",
	"28",
	"29",
	"2a",
	"2b",
	"2c",
	"2d",
	"2e",
	"2f",
	"30",
	"31",
	"32",
	"33",
	"34",
	"35",
	"36",
	"37",
	"38",
	"39",
	"3a",
	"3b",
	"3c",
	"3d",
	"3e",
	"3f",
	"40",
	"41",
	"42",
	"43",
	"44",
	"45",
	"46",
	"47",
	"48",
	"49",
	"4a",
	"4b",
	"4c",
	"4d",
	"4e",
	"4f",
	"50",
	"51",
	"52",
	"53",
	"54",
	"55",
	"56",
	"57",
	"58",
	"59",
	"5a",
	"5b",
	"5c",
	"5d",
	"5e",
	"5f",
	"60",
	"61",
	"62",
	"63",
	"64",
	"65",
	"66",
	"67",
	"68",
	"69",
	"6a",
	"6b",
	"6c",
	"6d",
	"6e",
	"6f",
	"70",
	"71",
	"72",
	"73",
	"74",
	"75",
	"76",
	"77",
	"78",
	"79",
	"7a",
	"7b",
	"7c",
	"7d",
	"7e",
	"7f",
	"80",
	"81",
	"82",
	"83",
	"84",
	"85",
	"86",
	"87",
	"88",
	"89",
	"8a",
	"8b",
	"8c",
	"8d",
	"8e",
	"8f",
	"90",
	"91",
	"92",
	"93",
	"94",
	"95",
	"96",
	"97",
	"98",
	"99",
	"9a",
	"9b",
	"9c",
	"9d",
	"9e",
	"9f",
	"a0",
	"a1",
	"a2",
	"a3",
	"a4",
	"a5",
	"a6",
	"a7",
	"a8",
	"a9",
	"aa",
	"ab",
	"ac",
	"ad",
	"ae",
	"af",
	"b0",
	"b1",
	"b2",
	"b3",
	"b4",
	"b5",
	"b6",
	"b7",
	"b8",
	"b9",
	"ba",
	"bb",
	"bc",
	"bd",
	"be",
	"bf",
	"c0",
	"c1",
	"c2",
	"c3",
	"c4",
	"c5",
	"c6",
	"c7",
	"c8",
	"c9",
	"ca",
	"cb",
	"cc",
	"cd",
	"ce",
	"cf",
	"d0",
	"d1",
	"d2",
	"d3",
	"d4",
	"d5",
	"d6",
	"d7",
	"d8",
	"d9",
	"da",
	"db",
	"dc",
	"dd",
	"de",
	"df",
	"e0",
	"e1",
	"e2",
	"e3",
	"e4",
	"e5",
	"e6",
	"e7",
	"e8",
	"e9",
	"ea",
	"eb",
	"ec",
	"ed",
	"ee",
	"ef",
	"f0",
	"f1",
	"f2",
	"f3",
	"f4",
	"f5",
	"f6",
	"f7",
	"f8",
	"f9",
	"fa",
	"fb",
	"fc",
	"fd",
	"fe",
	"ff",
}

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

func binaryTopgSqlStr(s *str.Str, bytes []byte) {
	s.AppendString("'")
	for _, v := range bytes {
		s.AppendString(pgsqlByteToString[int(v)])
	}
	s.AppendString("'::bytea")
}

func binaryTomySqlStr(s *str.Str, bytes []byte) {
	s.AppendString("unhex('")
	for _, v := range bytes {
		s.AppendString(mysqlByteToString[int(v)])
	}
	s.AppendString("')")
}

/*
 *重放时对insert要使用updateinsert语句
 *INSERT INTO %s(%s) VALUES(%s) ON conflict(__key__)  DO UPDATE SET %s;
 */

func (this *sqlMgr) buildInsertUpdateStringPgSql(s *str.Str, kv *kv) {
	logger.Debugln("buildInsertUpdateStringPgSql")

	meta := kv.getMeta()

	version := proto.PackField("__version__", kv.version)

	s.AppendString(meta.GetInsertPrefix()).AppendString("'").AppendString(kv.key).AppendString("',") //add __key__
	s.AppendFieldStr(version, this.binaryToSqlStr).AppendString(",")                                 //add __version__

	//add other fields
	i := 0

	fields := meta.GetInsertOrder()

	for _, name := range fields {
		v, ok := kv.fields[name]
		if !ok {
			v = proto.PackField(name, kv.meta.GetDefaultV(name))
		}

		s.AppendFieldStr(v, this.binaryToSqlStr)
		if i != len(fields)-1 {
			s.AppendString(",")
		}
		i++
	}
	s.AppendString(") ON conflict(__key__)  DO UPDATE SET ")
	for _, v := range kv.fields {
		if meta.CheckFieldMeta(v) {
			s.AppendString(v.GetName()).AppendString("=").AppendFieldStr(v, this.binaryToSqlStr).AppendString(",")
		}
	}
	s.AppendString("__version__=").AppendFieldStr(version, this.binaryToSqlStr)
	s.AppendString(" where ").AppendString(kv.table).AppendString(".__key__ = '").AppendString(kv.key).AppendString("';")
	//Debugln(s.ToString())
}

/*
 *insert into %s(%s) values(%s) on duplicate key update %s;
 */

func (this *sqlMgr) buildInsertUpdateStringMySql(s *str.Str, kv *kv) {

	logger.Debugln("buildInsertUpdateStringMySql")

	meta := kv.getMeta()

	version := proto.PackField("__version__", kv.version)

	s.AppendString(meta.GetInsertPrefix()).AppendString("'").AppendString(kv.key).AppendString("',") //add __key__
	s.AppendFieldStr(version, this.binaryToSqlStr).AppendString(",")                                 //add __version__

	//add other fields
	fields := meta.GetInsertOrder()
	i := 0
	for _, name := range fields {
		v, ok := kv.fields[name]
		if !ok {
			v = proto.PackField(name, kv.meta.GetDefaultV(name))
		}
		s.AppendFieldStr(v, this.binaryToSqlStr)
		if i != len(fields)-1 {
			s.AppendString(",")
		}
		i++
	}

	s.AppendString(") on duplicate key update ")

	for _, v := range kv.fields {
		if meta.CheckFieldMeta(v) {
			s.AppendString(v.GetName()).AppendString("=").AppendFieldStr(v, this.binaryToSqlStr).AppendString(",")
		}
	}
	s.AppendString("__version__=").AppendFieldStr(version, this.binaryToSqlStr)
	s.AppendString(";")

}

func (this *sqlMgr) buildInsertString(s *str.Str, kv *kv) {
	meta := kv.getMeta()
	version := proto.PackField("__version__", kv.version)
	s.AppendString(meta.GetInsertPrefix()).AppendString("'").AppendString(kv.key).AppendString("',") //add __key__
	s.AppendFieldStr(version, this.binaryToSqlStr).AppendString(",")                                 //add __version__

	i := 0
	fields := meta.GetInsertOrder()
	for _, name := range fields {

		v, ok := kv.fields[name]
		if !ok {
			v = proto.PackField(name, kv.meta.GetDefaultV(name))
		}

		s.AppendFieldStr(v, this.binaryToSqlStr)
		if i != len(fields)-1 {
			s.AppendString(",")
		}
		i++
	}

	s.AppendString(");")
}

func (this *sqlMgr) buildUpdateString(s *str.Str, kv *kv) {
	meta := kv.getMeta()
	s.AppendString("update ").AppendString(kv.table).AppendString(" set ")
	version := proto.PackField("__version__", kv.version)
	if len(kv.modifyFields) > 0 {
		for k, v := range kv.modifyFields {
			if meta.CheckFieldMeta(v) {
				s.AppendString(k).AppendString("=").AppendFieldStr(v, this.binaryToSqlStr).AppendString(",")
			}
		}
	} else {
		for _, v := range kv.fields {
			if meta.CheckFieldMeta(v) {
				s.AppendString(v.GetName()).AppendString("=").AppendFieldStr(v, this.binaryToSqlStr).AppendString(",")
			}
		}
	}

	s.AppendString("__version__=").AppendFieldStr(version, this.binaryToSqlStr)
	s.AppendString(" where __key__ = '").AppendString(kv.key).AppendString("';")

	//Debugln(s.ToString())
}

func (this *sqlMgr) buildDeleteString(s *str.Str, ckey *kv) {
	s.AppendString("delete from ").AppendString(ckey.table).AppendString(" where __key__ = '").AppendString(ckey.key).AppendString("';")
}
