/////////// Filtering

// Textfile from HDFS
val rdd = sc.textFile("/user/komlosila/Spark/Orders/orders.csv")

// Sample data
rdd.take(10)
rdd.take(10).foreach(println)

// Header
val header = rdd.first

// Getting rid of header
val headlessrdd = rdd.filter(sale => { sale != header })

// Sample headless
headlessrdd.first

// Headless rowcount
headlessrdd.count

// Filter
val filteredrdd = headlessrdd.filter( row => {
val region = row.split(",")(12)
region == "West" || region == "East"
})

// Sample filtered rdd
filteredrdd.take(10).foreach(println)

// Filtered rdd rowcount
filteredrdd.count

// Defining a filter function
def filterFunc(sale: String): Boolean = {
    val region = sale.split(",")(12)
    region == "West" || region == "East"
}

// Filtered rdd with filter function
val filteredrdd = headlessrdd.filter(filterFunc)


// Filtered rdd with filter function
val filteredrdd = headlessrdd.filter(sale => {
    val dateList = sale.split(",")(2).split("/")
    val year = dateList(2).toInt
    val month = dateList(1).toInt    
    val day = dateList(0).toInt
    year >= 16 && month >=1 && day >= 1
})

// Sample filtered rdd
filteredrdd.take(10).foreach(println)