package main

import "fmt"
import "time"
import "strconv"

//import "reflect"
import "database/sql"

import _ "github.com/go-sql-driver/mysql"
import "github.com/Pallinder/go-randomdata"

const DB_CONN = "herl_user:notsecure@tcp(herl-mysql.c7glrswbovia.us-east-1.rds.amazonaws.com:3306)/herl" //yes, this is public, no it's not great but I'm also not depending on the password for security here
const NUMBER_OF_THREADS = 10
const NUMBER_OF_USERS = 10000

func dbConn() *sql.DB {

    db, err := sql.Open("mysql", DB_CONN)
    if err != nil {
        panic(err)
    }

    err = db.Ping()
    if err != nil {
        panic(err)
    }

    return db
}

func makeTable(db *sql.DB) error {
    var create_table_sql string = "CREATE TABLE `herl`.`users` ("       +
                                    "`id` INT NOT NULL AUTO_INCREMENT," +
                                    "`email` VARCHAR(45) NOT NULL,"     +
                                    "`firstname` VARCHAR(45) NOT NULL," +
                                    "`lastname` VARCHAR(45) NOT NULL,"  +
                                    "PRIMARY KEY (`id`));"

    createResult, err := db.Query(create_table_sql)

    if err != nil {
            return err
    }

    createResult.Close()

    return nil
}

func showTables(db *sql.DB) error {
	tableRows, err := db.Query("show tables;")

	if err != nil {
		return err
	}
	defer tableRows.Close()

	var table string
	for tableRows.Next() {
		fmt.Println("iterate!")
		tableRows.Scan(&table)
		fmt.Println(table)
	}

	return nil
}

func dropTable(db *sql.DB) error {
	dropResult, err := db.Query("DROP TABLE `herl`.`users`;")
	if err != nil {
		return err
	}
	dropResult.Close()

	return nil
}

func dropTableClosure(db *sql.DB) {
	err := dropTable(db)
	if err != nil {
		panic(err)
	}
}

func createUsers(results chan<- bool) {
	db := dbConn()
	defer db.Close()

	insert, err := db.Prepare("INSERT INTO `herl`.`users` (`email`, `firstname`, `lastname`) VALUES( ?, ?, ? )")
	if err != nil {
		panic(err)
	}

	for i := 0; i < (NUMBER_OF_USERS / NUMBER_OF_THREADS); i++ {
		_, err = insert.Exec(randomdata.Email(), randomdata.FirstName(randomdata.RandomGender), randomdata.LastName())
		if err != nil {
			panic(err)
		}
		results <- true
	}
}

func pollDBSize(tick <-chan time.Time, stop <-chan bool) {
    //fmt.Println("pollDBSize start")
    //defer fmt.Println("pollDBSize stop")

    db := dbConn()
    defer db.Close()
    
    //iterate while users table is being loaded
    for {
        <-tick 
        
        select {
        case <-stop:
            return
        default:
            var count int64
            result, err := db.Query("select count(*) from `herl`.`users`;")
            if err != nil {
                    panic(err)
            }
            for result.Next() {
                err = result.Scan(&count)
                if err != nil {
                    panic(err)
                }
                
                
                fmt.Println("number of users: " + strconv.FormatInt(count, 10) + "/" + strconv.FormatInt(NUMBER_OF_USERS, 10))
                result.Close()
            }
        }
    }
}

func prepare() *sql.DB {
    fmt.Println("Starting preparation phase")
    
    //create a db connection
    db := dbConn()
    
    //drop our table if it exists -- if it doesn't, whatever
    _ = dropTable(db)

    //create a new table
    err := makeTable(db)
    if err != nil {
        panic(err.Error())
    }
    
    fmt.Println("Preparation phase complete")
    
    return db
}

func generateData(db *sql.DB) {
    
    fmt.Println("Starting generate data phase")
    
    //Generate data across as many threads as is configured to use
    results := make(chan bool, NUMBER_OF_USERS)
    for i := 0; i < NUMBER_OF_THREADS; i++ {
        go createUsers(results)
    }

    tick := time.Tick(time.Second)
    stop := make(chan bool, 1)

    go func() {
        //join the goroutines
        for j := 0; j < NUMBER_OF_USERS; j++ {
                <-results
        }
        
        //stop our polling
        stop <- true
    }()

    //status updates
    pollDBSize(tick, stop)
    
    fmt.Println("Data generation complete")

}

func main() {
        db := prepare()
        defer db.Close()
        
        generateData(db)

        //generate data 
	
}
