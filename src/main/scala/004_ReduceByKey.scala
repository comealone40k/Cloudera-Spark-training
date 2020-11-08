////////////// ReduceByKey

// Textfile from HDFS
val flights = sc.textFile("/user/komlosila/Spark/Flights/flights.csv")
flights.take(10).foreach(println)

// Removing header row
val header = flights.first
val headlessflights = flights.filter(sale => { sale != header })

// Regexp to properly split comma separated text
val r = """,(?=([^\"]*\"[^\"]*\")*[^\"]*$)"""

val filteredFlights = flights.filter(flight => {
    flight != header && flight.split(r)(22) != ""
})

filteredFlights.take(10).foreach(println)

// Total miles travelled
val milesTotal = filteredFlights.map(flight => flight.split(r)(7)).reduce((sum, current) => sum + current)

// Total Flights in a year
val flightsTotal = filteredFlights.map(flight => 1).reduce((sum, current) => sum + current)
val flightsTotalCount = filteredFlights.map(flight => (flight.split(r)(1).toInt, flight.split(r)(17).toLong)).count

val flightsYearDist = filteredFlights.map(flight => (flight.split(r)(0).toInt, flight.split(r)(17).toLong))
flightsYearDist.take(10).foreach(println)

// Average miles per flight
val avg_miles_per_flight = milesTotal / flightsTotalCount

// Flights per airport
// val airports = sc.textFile("/user/komlosila/Spark/Flights/airports.csv")
// airports.take(10).foreach(println)

val flights_per_airport = 
filteredFlights.map(flight => (flight.split(r)(7),1)).
  reduceByKey((sum, current) => sum + current).
  map( airport => (-airport._2, airport._1)).  // Decending order
  sortByKey().
  map( airport => (airport._2, -airport._1))   // multiply negative value back to positive
  
flights_per_airport.take(10).foreach(println)