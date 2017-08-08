# min

## orm

A simple lightweight database query framework

```go

type User struct {
	Id         int
	Username   string
	Password   string
	CreateTime time.Time `col:"create_time"`
	UpdateTime time.Time `col:"update_time"`
}

db, err := orm.NewDB("mysql", "root:123456@tcp(127.0.0.1:3306)/demo")
defer db.Close()

sql := db.NewSql().
	InsertInto("user").
	Columns("username, password, create_time").
	Values("${Username}, ${Password}, ${CreateTime}")
sql.Prepare()
defer sql.Close()
id := sql.Exec(User{
	Username:   "hello",
	Password:   "world",
	CreateTime: time.Now(),
})
fmt.Println("id:", id)

var users []User

err = db.NewSql().
	Select("*").
	From("user").
	Where("create_time > ${CreateTime}").
	QueryRows(struct{ CreateTime string }{CreateTime: "2017-01-01 00:00:00"}, &users)

if err != nil {
	fmt.Println("users:", users)
} else {
	fmt.Println("err:", err)
}

```