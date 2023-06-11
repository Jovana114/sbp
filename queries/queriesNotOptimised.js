//prvi upit
 db.feedback.aggregate([
  {
    $lookup: {
      from: "product_info",
      localField: "product_id",
      foreignField: "_id",
      as: "product_info"
    }
  },
  {
    $unwind: "$product_info"
  },
  {
    $match: {
      "product_info.secondary_category": "Moisturizers",
      is_recommended: "1.0"
    }
  },
  {
    $group: {
      _id: {
        skin_type: "$skin_type",
        product_id: "$product_id",
        product_name: "$product_info.product_name",
        brand_name: "$product_info.brand_name"
      },
      count: {
        $sum: 1
      }
    }
  },
  {
    $sort: {
      "_id.skin_type": 1,
      count: -1
    }
  },
  {
    $group: {
      _id: "$_id.skin_type",
      products: {
        $push: {
          product_id: "$_id.product_id",
          product_name: "$_id.product_name",
          brand_name: "$_id.brand_name",
          count: "$count"
        }
      }
    }
  },
  {
    $project: {
      _id: 1,
      products: {
        $slice: ["$products", 0, 5]
      }
    }
  }
]);

//drugi upit
db.feedback.aggregate([
  {
    $lookup: {
      from: "product_info",
      localField: "product_id",
      foreignField: "_id",
      as: "product_info"
    }
  },
  {
    $unwind: "$product_info"
  },
  {
    $group: {
      _id: "$product_id",
      average_rating: { $avg: "$rating" },
      product_name: { $first: "$product_info.product_name" },
      brand_name: { $first: "$product_info.brand_name" },
      recommendation_count: {
        $sum: {
          $cond: [
            { $ne: ["$is_recommended", ""] },
            { $toDouble: "$is_recommended" },
            0
          ]
        }
      }
    }
  },
  {
    $project: {
      _id: 1,
      average_rating: { $round: ["$average_rating", 2] },
      product_name: 1,
      brand_name: 1,
      recommendation_count: 1
    }
  }
]);

//treci upit
db.feedback.aggregate([
  {
    $lookup: {
      from: "product_info",
      localField: "product_id",
      foreignField: "_id",
      as: "product_info"
    }
  },
  {
    $unwind: "$product_info"
  },
  {
    $group: {
      _id: "$product_info.brand_name",
      recommendation_count: {
        $sum: {
          $cond: [
            { $ne: ["$is_recommended", ""] },
            { $toDouble: "$is_recommended" },
            0
          ]
        }
      },
      total_neg_feedback_count: { $sum: "$total_neg_feedback_count" },
      total_pos_feedback_count: { $sum: "$total_pos_feedback_count" }
    }
  },
  {
    $project: {
      _id: 0,
      brand_name: "$_id",
      recommendation_count: 1,
      total_neg_feedback_count: 1,
      total_pos_feedback_count: 1
    }
  }
]);

//cetvrti upit
db.feedback.aggregate([
  {
    $lookup: {
      from: "product_info",
      localField: "product_id",
      foreignField: "_id",
      as: "product_info"
    }
  },
  {
    $unwind: "$product_info"
  },
  {
    $match: {
      is_recommended: "1.0"
    }
  },
  {
    $group: {
      _id: {
        brand_name: "$product_info.brand_name",
        skin_tone: "$skin_tone",
        skin_type: "$skin_type",
        hair_color: "$hair_color"
      },
      positive_reviews: { $sum: 1 }
    }
  },
  {
    $sort: {
      "_id.brand_name": 1,
      positive_reviews: -1
    }
  },
  {
    $group: {
      _id: "$_id.brand_name",
      profiles: {
        $push: {
          skin_tone: "$_id.skin_tone",
          skin_type: "$_id.skin_type",
          hair_color: "$_id.hair_color",
          positive_reviews: "$positive_reviews"
        }
      }
    }
  },
  {
    $project: {
      _id: 0,
      brand_name: "$_id",
      profiles: {
        $slice: ["$profiles", 0, 1]
      }
    }
  }
]);

//peti upit
db.feedback.aggregate([
  {
    $lookup: {
      from: "product_info",
      localField: "product_id",
      foreignField: "_id",
      as: "product_info"
    }
  },
  {
    $unwind: "$product_info"
  },
  {
    $match: {
      is_recommended: "1.0",
      "product_info.brand_name": { $ne: "" }
    }
  },
  {
    $group: {
      _id: {
        brandName: "$product_info.brand_name",
        productId: "$product_info._id",
        productName: "$product_info.product_name",
        primaryCategory: "$product_info.primary_category",
        secondaryCategory: "$product_info.secondary_category",
        tertiaryCategory: "$product_info.tertiary_category"
      },
      avgRating: {
        $avg: { $toDouble: "$rating" }  // Calculate the average rating using $avg and $toDouble
      },
      totalRecommendations: { $sum: 1 }  // Count the number of recommendations
    }
  },
  {
    $sort: {
      "_id.brandName": 1,
      avgRating: -1,  // Sort by average rating in descending order
      totalRecommendations: -1  // Sort by total recommendations in descending order
    }
  },
  {
    $group: {
      _id: "$_id.brandName",
      topProduct: {
        $first: {
          productId: "$_id.productId",
          productName: "$_id.productName",
          primaryCategory: "$_id.primaryCategory",
          secondaryCategory: "$_id.secondaryCategory",
          tertiaryCategory: "$_id.tertiaryCategory",
          avgRating: { $divide: [ { $round: ["$avgRating", 1] }, 1 ] },  // Round average rating to 1 decimal place
          totalRecommendations: "$totalRecommendations"
        }
      }
    }
  },
  {
    $project: {
      _id: 0,
      brandName: "$_id",
      topProduct: 1
    }
  }
]);

//for every hair color and for every skin type and every eye color find brand with max value_counts
db.feedback.aggregate([
  {
    $lookup: {
      from: "product_info",
      localField: "product_id",
      foreignField: "_id",
      as: "product"
    }
  },
  {
    $unwind: "$product"
  },
  {
    $group: {
      _id: {
        hair_color: "$hair_color",
        skin_type: "$skin_type",
        eye_color: "$eye_color",
        brand_id: "$product.brand_id"
      },
      count: { $sum: 1 }
    }
  },
  {
    $sort: {
      "_id.hair_color": 1,
      "_id.skin_type": 1,
      "_id.eye_color": 1,
      count: -1
    }
  },
  {
    $group: {
      _id: {
        hair_color: "$_id.hair_color",
        skin_type: "$_id.skin_type",
        eye_color: "$_id.eye_color"
      },
      max_count: { $first: "$count" },
      brand_id: { $first: "$_id.brand_id" }
    }
  },
  {
    $lookup: {
      from: "product_info",
      localField: "brand_id",
      foreignField: "brand_id",
      as: "brand"
    }
  },
  {
    $unwind: "$brand"
  },
  {
    $project: {
      _id: 0,
      hair_color: "$_id.hair_color",
      skin_type: "$_id.skin_type",
      eye_color: "$_id.eye_color",
      brand_id: "$brand.brand_id",
      brand_name: "$brand.brand_name",
      max_count: 1
    }
  }
]);

//for all feedbacks in 2022, where "is_recommended" : "1.0", for every product from tertiary_category and for every brand in it find  average rating 
db.feedback.aggregate([
  {
    $match: {
      submission_time: {
        $gte: ISODate("2022-01-01T00:00:00.000Z"),
        $lt: ISODate("2023-01-01T00:00:00.000Z")
      },
      is_recommended: "1.0"
    }
  },
  {
    $lookup: {
      from: "product_info",
      localField: "product_id",
      foreignField: "_id",
      as: "product"
    }
  },
  {
    $unwind: "$product"
  },
  {
    $match: {
      "product.tertiary_category": { $ne: "" },
      "product.brand_id": { $ne: NumberInt(0) }
    }
  },
  {
    $group: {
      _id: {
        tertiaryCategory: "$product.tertiary_category",
        brandId: "$product.brand_id",
        brandName: "$product.brand_name"
      },
      avgRating: {
        $avg: "$rating"
      }
    }
  },
  {
    $group: {
      _id: "$_id.tertiaryCategory",
      brands: {
        $push: {
          brandId: "$_id.brandId",
          brandName: "$_id.brandName",
          avgRating: "$avgRating"
        }
      }
    }
  },
  {
    $project: {
      _id: 0,
      tertiaryCategory: "$_id",
      brands: 1
    }
  }
]);

//za svaki produkt iz sve tri kategorije i svaki brend u okviru njih koji ima limited_edition naci profil svake osobe (kombinacija boje kože, tipa kože, boje očiju i boje kose) koji je ostavila najvise total_pos_feedback_count

db.product_info.aggregate([
  {
    $match: {
      limited_edition: 1 // Filter for limited edition products
    }
  },
  {
    $lookup: {
      from: "feedback",
      localField: "_id",
      foreignField: "product_id",
      as: "feedbacks"
    }
  },
  {
    $unwind: "$feedbacks"
  },
  {
    $group: {
      _id: {
        product_id: "$_id",
        brand_name: "$brand_name"
      },
      profiles: {
        $push: {
          skin_color: "$feedbacks.skin_color",
          skin_type: "$feedbacks.skin_type",
          eye_color: "$feedbacks.eye_color",
          hair_color: "$feedbacks.hair_color",
          total_pos_feedback_count: "$feedbacks.total_pos_feedback_count"
        }
      }
    }
  },
  {
    $project: {
      _id: 0,
      product_id: "$_id.product_id",
      brand_name: "$_id.brand_name",
      profiles: {
        $reduce: {
          input: "$profiles",
          initialValue: { total_pos_feedback_count: 0 },
          in: {
            $cond: {
              if: { $gt: ["$$this.total_pos_feedback_count", "$$value.total_pos_feedback_count"] },
              then: "$$this",
              else: "$$value"
            }
          }
        }
      }
    }
  },
  {
    $group: {
      _id: "$product_id",
      brands: {
        $push: {
          brand_name: "$brand_name",
          profiles: "$profiles"
        }
      }
    }
  },
  {
    $group: {
      _id: null,
      products: {
        $push: {
          product_id: "$_id",
          brands: "$brands"
        }
      }
    }
  }
]);

//za svaki skin_tone i skin_type i svaki produkt iz tertiary_category = tertiary_category naci brandove sa najjeftinijim prozivodima i izracunati prosecan total_pos_feedback_count ya njih

db.feedback.aggregate([
  {
    $lookup: {
      from: "product_info",
      localField: "product_id",
      foreignField: "_id",
      as: "product_info"
    }
  },
  {
    $unwind: "$product_info"
  },
  {
    $match: {
      "product_info.tertiary_category": { $ne: "" },
      "skin_tone": { $ne: "" },
      "skin_type": { $ne: "" }
    }
  },
  {
    $group: {
      _id: {
        tertiary_category: "$product_info.tertiary_category",
        skin_tone: "$skin_tone",
        skin_type: "$skin_type"
      },
      min_price: { $min: "$product_info.price_usd" },
      brands: { $addToSet: "$product_info.brand_name" }
    }
  },
  {
    $project: {
      _id: 0,
      tertiary_category: "$_id.tertiary_category",
      skin_tone: "$_id.skin_tone",
      skin_type: "$_id.skin_type",
      min_price: 1,
      brands: 1
    }
  }
]);

//za svaki rating iz feedback-a i za svaki brand, naci po proizvod iz svake kategorije koji ima najvecu cenu
db.feedback.aggregate([
  {
    $lookup: {
      from: "product_info",
      localField: "product_id",
      foreignField: "_id",
      as: "product_info"
    }
  },
  {
    $unwind: "$product_info"
  },
  {
    $group: {
      _id: {
        rating: "$rating",
        brand: "$product_info.brand_name"
      },
      max_price: { $max: "$product_info.price_usd" },
      products: { $push: "$product_info" }
    }
  },
  {
    $unwind: "$products"
  },
  {
    $match: {
      $expr: {
        $eq: ["$products.price_usd", "$max_price"]
      }
    }
  },
  {
    $project: {
      _id: 0,
      rating: "$_id.rating",
      brand: "$_id.brand",
      product: "$products"
    }
  }
]);

