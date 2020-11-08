/////////// Mapping

// ( key, value )
// Importing the Data
val ordersRdd = sc.textFile("/user/komlosila/Spark/Orders/orders.csv")

// Removing header row
val header = ordersRdd.first
val headlessRdd = ordersRdd.filter(sale => { sale != header })

// Filtering by Date
val filteredRdd = headlessRdd.filter(sale => {
    val dateList = sale.split(",")(2).split("/")
    val year = dateList(2).toInt
    val month = dateList(1).toInt    
    val day = dateList(0).toInt
    year >= 16 && month >=1 && day >= 1
})


// Sample filtered rdd
filteredRdd.take(10).foreach(println)

// Map RDD
val productinfo = filteredRdd.map(sale => {
    (sale.split(",")(13),sale.split(",")(16))
})
productinfo.take(10).foreach(println)

// Complex Mapping
val salesNzumbersRdd = filteredRdd.map( sale => {
    val saleList = sale.split(",")
    val orderId = saleList(1)
    val saleAmount = saleList(17)
    val quantity = saleList(18)
    val discount = saleList(19)
    val profit = saleList(20)
    ( orderId, (saleAmount, quantity, discount, profit))
})

// Show results
salesNzumbersRdd.take(20).foreach(println)

// There are many errors, let's see why
val sample = ordersRdd.take(2501)(2500)

// Correct mapping with commas in text
val saleList = sample.split(",")
val headerSize = header.split(",").size
saleList.size

val r = """,(?=([^\"]*\"[^\"]*\")*[^\"]*$)"""
val correct_split = sample.split(r)

val r = """,(?=([^\"]*\"[^\"]*\")*[^\"]*$)"""

// Complex Mapping with correct split
val salesNumbersRdd = filteredRdd.map( sale => {
    val saleList = sale.split(r)
    val orderId = saleList(1)
    val saleAmount = saleList(17)
    val quantity = saleList(18)
    val discount = saleList(19)
    val profit = saleList(20)
    ( orderId, (saleAmount, quantity, discount, profit))
})

// Show results
salesNumbersRdd.take(20).foreach(println)