const mongodb= require('mongodb')
const async = require('async')

const url = 'mongodb://localhost:27017'
const dbName = 'edx-course-db'
const collectionName = 'customers'
let customers = require('./m3-customer-data')
let addresses = require('./m3-customer-address-data')
let numOfObjects = parseInt(process.argv[2], 10) || customers.length

mongodb.MongoClient.connect(url, { useNewUrlParser: true }, (error, client) => {
    console.log('Open MongoDB connection')
    if (error){
        console.error(error)
        return process.exit(1)
    }
    const db = client.db(dbName)
    let asyncTasks = []

    customers.forEach((customer, index) => {
        let start, limit
        customers[index] = Object.assign(customer, addresses[index])
        if(index % numOfObjects === 0){
            start = index
            limit = start+numOfObjects
            limit = (limit > customers.length) ? customers.length : limit

            asyncTasks.push((callback) => {
                console.log(`Inserting records ${start}/${limit} of ${customers.length}`)
                db.collection(collectionName).insertMany(customers.slice(start, limit), (error, results) => {
                    callback(error, results)
                })
            })
        }
    })
    async.parallel(asyncTasks, (error, results) => {
        if (error){
            console.error(error)
        }
        results.forEach((result, index) => {
            console.log(`Pass ${index}: Inserted ${result.result.n} records`)
        })
        console.log('Close MongoDB connection')
        client.close()
    })
})