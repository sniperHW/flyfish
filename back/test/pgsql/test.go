package main

import (
	"database/sql"
	"fmt"
	//"time"
	//"encoding/binary"
	_ "github.com/lib/pq"
	//"time"
	//"math/rand"
	//"sync/atomic"
	//"os"
)

func main() {
	connStr := "host=10.128.2.166 port=5432 dbname=wei user=dbuser password=123456 sslmode=disable" //fmt.Sprintf("dbname=%s user=%s password=%s sslmode=disable", os.Args[1], os.Args[2], os.Args[3])
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		fmt.Println(err)
		return
	}

	//str := "INSERT INTO users1(__key__,__version__,name,age,phone,blob) VALUES('dd1',1,'dd1','40','12334','\\075'::bytea);"

	str := "update users1 set __version__=1,name='huangwie',age=37,phone='\\075'::bytea where __key__ = 'huangwei';"

	fmt.Println(str)

	_, err = db.Exec(str)

	if nil == err {
		fmt.Println("ok")
	} else {
		fmt.Println(err)
	}

	//buff := make([]byte, 4)
	//binary.BigEndian.PutUint32(buff, 100)

	//str := "INSERT INTO users1(__key__,__version__,name,age,phone,blobdata) VALUES('dd1',1,'dd1','40','12334','"
	//str += string(buff)
	//str += "'::bytea);"

	/*str := "INSERT INTO users1(__key__,__version__,name,age,phone,blobdata) VALUES('dd1',1,'dd1','40','12334','d'::bytea);"

	fmt.Println(str)

	_, err = db.Exec(str)

	if nil == err {
		fmt.Println("ok")
	} else {
		fmt.Println(err)
	}*/

	//db.Ping()

	//_, err = db.Exec("delete from users1;")

	/*	beg := time.Now()

		_, err = db.Exec(`
				insert into users1 values('ca1','30','123');
				insert into users1 values('ca2','30','123');
				insert into users1 values('ca3','30','123');
				insert into users1 values('ca4','30','123');
				insert into users1 values('ca5','30','123');
				insert into users1 values('ca6','30','123');
				insert into users1 values('ca7','30','123');
				insert into users1 values('ca8','30','123');
				insert into users1 values('ca9','30','123');
				insert into users1 values('ca10','30','123');
				insert into users1 values('ca11','30','123');
				insert into users1 values('ca12','30','123');
				insert into users1 values('ca13','30','123');
				insert into users1 values('ca14','30','123');
				insert into users1 values('ca15','30','123');
				insert into users1 values('ca16','30','123');
				insert into users1 values('ca17','30','123');
				insert into users1 values('ca18','30','123');
				insert into users1 values('ca19','30','123');
				insert into users1 values('ca20','30','123');
				insert into users1 values('ca21','30','123');
				insert into users1 values('ca22','30','123');
				insert into users1 values('ca23','30','123');
				insert into users1 values('ca24','30','123');
				insert into users1 values('ca25','30','123');
				insert into users1 values('ca26','30','123');
				insert into users1 values('ca27','30','123');
				insert into users1 values('ca28','30','123');
				insert into users1 values('ca29','30','123');
				insert into users1 values('ca30','30','123');
				insert into users1 values('cca1','30','123');
				insert into users1 values('cca2','30','123');
				insert into users1 values('cca3','30','123');
				insert into users1 values('cca4','30','123');
				insert into users1 values('cca5','30','123');
				insert into users1 values('cca6','30','123');
				insert into users1 values('cca7','30','123');
				insert into users1 values('cca8','30','123');
				insert into users1 values('cca9','30','123');
				insert into users1 values('cca10','30','123');
				insert into users1 values('cca11','30','123');
				insert into users1 values('cca12','30','123');
				insert into users1 values('cca13','30','123');
				insert into users1 values('cca14','30','123');
				insert into users1 values('cca15','30','123');
				insert into users1 values('cca16','30','123');
				insert into users1 values('cca17','30','123');
				insert into users1 values('cca18','30','123');
				insert into users1 values('cca19','30','123');
				insert into users1 values('cca20','30','123');
				insert into users1 values('cca21','30','123');
				insert into users1 values('cca22','30','123');
				insert into users1 values('cca23','30','123');
				insert into users1 values('cca24','30','123');
				insert into users1 values('cca25','30','123');
				insert into users1 values('cca26','30','123');
				insert into users1 values('cca27','30','123');
				insert into users1 values('cca28','30','123');
				insert into users1 values('cca29','30','123');
				insert into users1 values('cca30','30','123');
		`)

		if nil != err {
			fmt.Println(err)
		}

		fmt.Println(time.Now().Sub(beg))

		beg = time.Now()

		_, err = db.Exec("insert into users1 values('a1','30','123');")
		_, err = db.Exec("insert into users1 values('a2','30','123');")
		_, err = db.Exec("insert into users1 values('a3','30','123');")
		_, err = db.Exec("insert into users1 values('a4','30','123');")
		_, err = db.Exec("insert into users1 values('a5','30','123');")
		_, err = db.Exec("insert into users1 values('a6','30','123');")
		_, err = db.Exec("insert into users1 values('a7','30','123');")
		_, err = db.Exec("insert into users1 values('a8','30','123');")
		_, err = db.Exec("insert into users1 values('a9','30','123');")
		_, err = db.Exec("insert into users1 values('a10','30','123');")
		_, err = db.Exec("insert into users1 values('a11','30','123');")
		_, err = db.Exec("insert into users1 values('a12','30','123');")
		_, err = db.Exec("insert into users1 values('a13','30','123');")
		_, err = db.Exec("insert into users1 values('a14','30','123');")
		_, err = db.Exec("insert into users1 values('a15','30','123');")
		_, err = db.Exec("insert into users1 values('a16','30','123');")
		_, err = db.Exec("insert into users1 values('a17','30','123');")
		_, err = db.Exec("insert into users1 values('a18','30','123');")
		_, err = db.Exec("insert into users1 values('a19','30','123');")
		_, err = db.Exec("insert into users1 values('a20','30','123');")
		_, err = db.Exec("insert into users1 values('a21','30','123');")
		_, err = db.Exec("insert into users1 values('a22','30','123');")
		_, err = db.Exec("insert into users1 values('a23','30','123');")
		_, err = db.Exec("insert into users1 values('a24','30','123');")
		_, err = db.Exec("insert into users1 values('a25','30','123');")
		_, err = db.Exec("insert into users1 values('a26','30','123');")
		_, err = db.Exec("insert into users1 values('a27','30','123');")
		_, err = db.Exec("insert into users1 values('a28','30','123');")
		_, err = db.Exec("insert into users1 values('a29','30','123');")
		_, err = db.Exec("insert into users1 values('a30','30','123');")

		_, err = db.Exec("insert into users1 values('aa1','30','123');")
		_, err = db.Exec("insert into users1 values('aa2','30','123');")
		_, err = db.Exec("insert into users1 values('aa3','30','123');")
		_, err = db.Exec("insert into users1 values('aa4','30','123');")
		_, err = db.Exec("insert into users1 values('aa5','30','123');")
		_, err = db.Exec("insert into users1 values('aa6','30','123');")
		_, err = db.Exec("insert into users1 values('aa7','30','123');")
		_, err = db.Exec("insert into users1 values('aa8','30','123');")
		_, err = db.Exec("insert into users1 values('aa9','30','123');")
		_, err = db.Exec("insert into users1 values('aa10','30','123');")
		_, err = db.Exec("insert into users1 values('aa11','30','123');")
		_, err = db.Exec("insert into users1 values('aa12','30','123');")
		_, err = db.Exec("insert into users1 values('aa13','30','123');")
		_, err = db.Exec("insert into users1 values('aa14','30','123');")
		_, err = db.Exec("insert into users1 values('aa15','30','123');")
		_, err = db.Exec("insert into users1 values('aa16','30','123');")
		_, err = db.Exec("insert into users1 values('aa17','30','123');")
		_, err = db.Exec("insert into users1 values('aa18','30','123');")
		_, err = db.Exec("insert into users1 values('aa19','30','123');")
		_, err = db.Exec("insert into users1 values('aa20','30','123');")
		_, err = db.Exec("insert into users1 values('aa21','30','123');")
		_, err = db.Exec("insert into users1 values('aa22','30','123');")
		_, err = db.Exec("insert into users1 values('aa23','30','123');")
		_, err = db.Exec("insert into users1 values('aa24','30','123');")
		_, err = db.Exec("insert into users1 values('aa25','30','123');")
		_, err = db.Exec("insert into users1 values('aa26','30','123');")
		_, err = db.Exec("insert into users1 values('aa27','30','123');")
		_, err = db.Exec("insert into users1 values('aa28','30','123');")
		_, err = db.Exec("insert into users1 values('aa29','30','123');")
		_, err = db.Exec("insert into users1 values('aa30','30','123');")

		fmt.Println(time.Now().Sub(beg))
	*/
	/*	c := int32(0)
		for i:=1;i < 10;i++ {
			go func(){
				for {
					stmts := ""
					for i := 1;i < 50; i++ {
						id := rand.Int()%1000000
						stmt1 := fmt.Sprintf("INSERT INTO users(name,age,phone) VALUES('ak%d','40','12334') ON conflict(name)  DO UPDATE SET age = '40', phone ='5678';",id)
						stmts = stmts + stmt1
					}
					ret,err := db.Exec(stmts)
					if nil != err {
						fmt.Println(err)
						return
					}

					rows, _ := ret.RowsAffected()
					fmt.Println(rows)

					atomic.AddInt32(&c, 50)
				}
			}()
		}

		go func(){
			for {
				time.Sleep(time.Second)
				fmt.Println(c)
				atomic.StoreInt32(&c,0)
			}
		}()

		sigStop := make(chan bool)
		_, _ = <-sigStop
	*/

	/*	ret,err := db.Exec(`
			insert into users values('ca1','30','123');
			insert into users values('ca2','30','123');
			insert into users values('ca3','30','123');
	`)*/

	/*
	   ret,err := db.Exec(`
	   	delete from users where name = 'cb1';
	   `)

	   	if nil != err {
	   		fmt.Println("err exec",err)
	   		return
	   	}

	   	{
	   		rows, err := ret.RowsAffected()
	   		if err != nil {
	   			fmt.Println("err RowsAffected",err)
	   			return
	   		}

	   		fmt.Println(rows)
	   	}

	*/

	/*rows, err := db.Query(`
			INSERT INTO users(name,age,phone) VALUES('ak3','40','12334') ON conflict(name)  DO UPDATE SET age = '40', phone ='5678';
			SELECT * FROM users where name in('ak1','ak2','ak3');
	`)
	*/

	/*rows, err := db.Query(`
			SELECT * FROM users where name in('ak1','ak2','ak3');
			SELECT * FROM users1 where name in('ak1','ak2','ak3');
			SELECT * FROM users where name in('ak1','ak2','ak3');
	`)*/

	/*
		rows, err := db.Query(`
		begin
			SELECT * FROM users where name = 'ak1';
			SELECT * FROM users where name = 'ak2';
			SELECT * FROM users where name = 'ak3';
		end;
		`)
	*/

	/*if nil != err {
		fmt.Println(err)
		return
	}

	values := make([]interface{}, 4)
	values[0] = new(string)
	values[1] = new(string)
	values[2] = new(string)
	values[3] = new(string)
	*/
	//for rows.Next() {
	/*var name  string
	var age   string
	var phone string*/

	//	if err := rows.Scan(values...); err != nil {
	//		fmt.Println("scan error", err)
	//		return
	//	}
	//	fmt.Println(*values[0].(*string), *values[1].(*string), *values[2].(*string), *values[3].(*string))
	//}

	//if err := rows.Err(); err != nil {
	//	fmt.Println(err)
	//	return
	//}

	//if !rows.NextResultSet() {
	//	fmt.Println("err", rows.Err())
	//log.Fatal("expected more result sets", rows.Err())
	//}

	//for rows.Next() {
	/*var name  string
	var age   string
	var phone string*/

	//	if err := rows.Scan(values...); err != nil {
	//		fmt.Println("scan error", err)
	//		return
	//	}
	//	fmt.Println(*values[0].(*string), *values[1].(*string), *values[2].(*string), *values[3].(*string))
	//}

	//if !rows.NextResultSet() {
	//	fmt.Println("err", rows.Err())
	//log.Fatal("expected more result sets", rows.Err())
	//}

	//for rows.Next() {
	/*var name  string
	var age   string
	var phone string*/

	//	if err := rows.Scan(values...); err != nil {
	//		fmt.Println("scan error", err)
	//		return
	//	}
	//	fmt.Println(*values[0].(*string), *values[1].(*string), *values[2].(*string), *values[3].(*string))
	//}

}

//INSERT INTO users(name,age,phone) VALUES('ak1','40','12334') ON conflict(name)  DO UPDATE SET age = '40', phone ='5678';
