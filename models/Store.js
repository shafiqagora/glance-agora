const mongoose = require('mongoose')
const Schema = mongoose.Schema

const storeSchema = new Schema(
  {
    products: [
      {
        type: Schema.ObjectId,
        ref: 'Product',
      },
    ],
    name: String,
    storeTemplate: String,
    storeType: String,
    storeUrl: String,
    yotpoId: String,
    logoImage: String,
    headerImage: String,
    city: String,
    state: String,
    country: String,
    isScrapped: { type: Boolean, default: false },
    deletedProducts: [
      {
        type: Schema.ObjectId,
        ref: 'Product',
      },
    ],
    latitude: Number,
    longitude: Number,
    creator: {
      type: Schema.Types.ObjectId,
      ref: 'User',
    },
    tags: [
      {
        type: String,
      },
    ],
    returnPolicy: String,
  },
  { timestamps: true }
)

storeSchema.set('toJSON', { getters: true })
module.exports = mongoose.model('store', storeSchema)
