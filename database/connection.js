const mongoose = require('mongoose')
require('dotenv').config()

const connectDB = async () => {
  try {
    const conn = await mongoose.connect(
      process.env.MONGODB_URI || 'mongodb://localhost:27017/shopify_crawler',
      {
        useNewUrlParser: true,
        useUnifiedTopology: true,
      }
    )

    console.log(`MongoDB Connected: ${conn.connection.host}`)
    return conn
  } catch (error) {
    console.error('Error connecting to MongoDB:', error.message)
    process.exit(1)
  }
}

const disconnectDB = async () => {
  try {
    await mongoose.disconnect()
    console.log('MongoDB Disconnected')
  } catch (error) {
    console.error('Error disconnecting from MongoDB:', error.message)
  }
}

module.exports = { connectDB, disconnectDB }
