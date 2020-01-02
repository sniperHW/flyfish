package db

import (
	"fmt"
	"testing"
)

func TestTruncate(t *testing.T) {
	client, err := GetClient("postgres@10.128.2.166@5432@deng@dbuser@123456")
	if err != nil {
		fmt.Println(1, err)
		return
	}

	err = client.Truncate("game_user")
	if err != nil {
		fmt.Println(2, err)
		return
	}

	fmt.Println("ok")
}

func TestCount(t *testing.T) {
	client, err := GetClient("postgres@10.128.2.166@5432@deng@dbuser@123456")
	if err != nil {
		fmt.Println(1, err)
		return
	}

	num, err := client.Count("game_user")
	if err != nil {
		fmt.Println(2, err)
		return
	}

	fmt.Println(num)
	num, err = client.Count("gameserver")
	if err != nil {
		fmt.Println(2, err)
		return
	}

	fmt.Println(num)
	fmt.Println("ok")
}

func TestGet(t *testing.T) {
	client, err := GetClient("postgres@10.128.2.166@5432@deng@dbuser@123456")
	if err != nil {
		fmt.Println(1, err)
		return
	}
	ret, err := client.GetAll("table_conf", []string{"__table__", "__conf__"})
	fmt.Println(ret, err)
}

func TestClient_DumpSql(t *testing.T) {
	client, err := GetClient("postgres@10.128.2.166@5432@deng@dbuser@123456")
	if err != nil {
		fmt.Println(1, err)
		return
	}
	ret, err := client.DumpSql(true)
	fmt.Println(ret, err)
}
