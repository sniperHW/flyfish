package main

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"

	//"time"
	//"math/rand"
	//"sync/atomic"
	"os"
)

func main() {
	connStr := fmt.Sprintf("dbname=%s user=%s password=%s sslmode=disable", os.Args[1], os.Args[2], os.Args[3])
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		fmt.Println(err)
		return
	}
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

	rows, err := db.Query(`
			SELECT * FROM users where name in('ak1','ak2','ak3'); 
			SELECT * FROM users1 where name in('ak1','ak2','ak3'); 
			SELECT * FROM users where name in('ak1','ak2','ak3');
	`)

	/*
		rows, err := db.Query(`
		begin
			SELECT * FROM users where name = 'ak1';
			SELECT * FROM users where name = 'ak2';
			SELECT * FROM users where name = 'ak3';
		end;
		`)
	*/
	if nil != err {
		fmt.Println(err)
		return
	}

	values := make([]interface{}, 4)
	values[0] = new(string)
	values[1] = new(string)
	values[2] = new(string)
	values[3] = new(string)

	for rows.Next() {
		/*var name  string
		var age   string
		var phone string*/

		if err := rows.Scan(values...); err != nil {
			fmt.Println("scan error", err)
			return
		}
		fmt.Println(*values[0].(*string), *values[1].(*string), *values[2].(*string), *values[3].(*string))
	}

	if err := rows.Err(); err != nil {
		fmt.Println(err)
		return
	}

	if !rows.NextResultSet() {
		fmt.Println("err", rows.Err())
		//log.Fatal("expected more result sets", rows.Err())
	}

	for rows.Next() {
		/*var name  string
		var age   string
		var phone string*/

		if err := rows.Scan(values...); err != nil {
			fmt.Println("scan error", err)
			return
		}
		fmt.Println(*values[0].(*string), *values[1].(*string), *values[2].(*string), *values[3].(*string))
	}

	if !rows.NextResultSet() {
		fmt.Println("err", rows.Err())
		//log.Fatal("expected more result sets", rows.Err())
	}

	for rows.Next() {
		/*var name  string
		var age   string
		var phone string*/

		if err := rows.Scan(values...); err != nil {
			fmt.Println("scan error", err)
			return
		}
		fmt.Println(*values[0].(*string), *values[1].(*string), *values[2].(*string), *values[3].(*string))
	}

}

//INSERT INTO users(name,age,phone) VALUES('ak1','40','12334') ON conflict(name)  DO UPDATE SET age = '40', phone ='5678';
