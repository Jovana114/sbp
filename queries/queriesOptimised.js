//prvi upit
db.product_info.aggregate([
  {
    $match: {
      "tertiary_category": "Moisturizers",
      "feedback.is_recommended": "1.0"
    }
  },
  {
    $unwind: "$feedback"
  },
  {
    $match: {
      "feedback.is_recommended": "1.0"
    }
  },
  {
    $group: {
      _id: {
        skin_type: "$feedback.skin_type",
        product_id: "$_id",
        product_name: "$product_name",
        brand_name: "$brand_name"
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

db.product_info.aggregate([
  {
    $addFields: {
      converted_is_recommended: {
        $map: {
          input: "$feedback.is_recommended",
          as: "rec",
          in: {
            $cond: [
              { $ne: ["$$rec", ""] },
              { $toDouble: "$$rec" },
              0
            ]
          }
        }
      }
    }
  },
  {
    $group: {
      _id: "$_id",
      average_rating: { $avg: "$feedback.rating" },
      product_name: { $first: "$product_name" },
      brand_name: { $first: "$brand_name" },
      recommendation_count: { $sum: "$converted_is_recommended" }
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
db.product_info.aggregate([
  {
    $unwind: "$feedback"
  },
  {
    $group: {
      _id: "$brand_name",
      recommendation_count: {
        $sum: {
          $cond: [
            { $ne: ["$feedback.is_recommended", ""] },
            { $toDouble: "$feedback.is_recommended" },
            0
          ]
        }
      },
      total_neg_feedback_count: { $sum: "$feedback.total_neg_feedback_count" },
      total_pos_feedback_count: { $sum: "$feedback.total_pos_feedback_count" }
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
db.product_info.aggregate([
  {
    $unwind: "$feedback"
  },
  {
    $match: {
      "feedback.is_recommended": "1.0"
    }
  },
  {
    $group: {
      _id: {
        brand_name: "$brand_name",
        skin_tone: "$feedback.skin_tone",
        skin_type: "$feedback.skin_type",
        hair_color: "$feedback.hair_color"
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
db.product_info.aggregate([
  {
    $unwind: "$feedback"
  },
  {
    $match: {
      "feedback.is_recommended": "1.0",
      brand_name: { $ne: "" }
    }
  },
  {
    $group: {
      _id: {
        brandName: "$brand_name",
        productId: "$_id",
        productName: "$product_name",
        primaryCategory: "$primary_category",
        secondaryCategory: "$secondary_category",
        tertiaryCategory: "$tertiary_category"
      },
      avgRating: {
        $avg: { $toDouble: "$feedback.rating" }
      },
      totalRecommendations: { $sum: 1 }
    }
  },
  {
    $sort: {
      "_id.brandName": 1,
      avgRating: -1,
      totalRecommendations: -1
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
          avgRating: { $divide: [{ $round: ["$avgRating", 1] }, 1] },
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
db.product_info.aggregate([
  {
    $unwind: "$feedback"
  },
  {
    $group: {
      _id: {
        hair_color: "$feedback.hair_color",
        skin_type: "$feedback.skin_type",
        eye_color: "$feedback.eye_color",
        brand_id: "$brand_id",
        brand_name: "$brand_name"
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
      brand_id: { $first: "$_id.brand_id" },
      brand_name: { $first: "$_id.brand_name" }
    }
  },
  {
    $project: {
      _id: 0,
      hair_color: "$_id.hair_color",
      skin_type: "$_id.skin_type",
      eye_color: "$_id.eye_color",
      brand_id: 1,
      brand_name: 1,
      max_count: 1
    }
  }
])

//for all feedbacks in 2022, where "is_recommended" : "1.0", for every product from tertiary_category and for every brand in it find  average rating 
db.product_info.aggregate([
  {
    $unwind: "$feedback"
  },
  {
    $match: {
      "feedback.is_recommended": "1.0",
      "feedback.submission_time": {
        $gte: ISODate("2022-01-01T00:00:00.000Z"),
        $lt: ISODate("2023-01-01T00:00:00.000Z")
      }
    }
  },
  {
    $group: {
      _id: {
        tertiary_category: "$tertiary_category",
        brand_name: "$brand_name"
      },
      average_rating: { $avg: "$feedback.rating" }
    }
  },
  {
    $project: {
      _id: 0,
      tertiary_category: "$_id.tertiary_category",
      brand_name: "$_id.brand_name",
      average_rating: 1
    }
  }
])

//za svaki produkt iz sve tri kategorije i svaki brend u okviru njih koji ima limited_edition naci profil svake osobe (kombinacija boje kože, tipa kože, boje očiju i boje kose) koji je ostavila najvise total_pos_feedback_count

db.product_info.aggregate([
  {
    $match: {
      limited_edition: 1
    }
  },
  {
    $unwind: "$feedback"
  },
  {
    $group: {
      _id: {
        tertiary_category: "$tertiary_category",
        brand_name: "$brand_name",
        product_id: "$product_id",
        skin_tone: "$feedback.skin_tone",
        skin_type: "$feedback.skin_type",
        eye_color: "$feedback.eye_color",
        hair_color: "$feedback.hair_color"
      },
      max_pos_feedback: { $max: "$feedback.total_pos_feedback_count" }
    }
  },
  {
    $group: {
      _id: {
        tertiary_category: "$_id.tertiary_category",
        brand_name: "$_id.brand_name",
        product_id: "$_id.product_id"
      },
      top_feedbacks: {
        $push: {
          skin_tone: "$_id.skin_tone",
          skin_type: "$_id.skin_type",
          eye_color: "$_id.eye_color",
          hair_color: "$_id.hair_color",
          total_pos_feedback_count: "$max_pos_feedback"
        }
      }
    }
  },
  {
    $project: {
      _id: 0,
      tertiary_category: "$_id.tertiary_category",
      brand_name: "$_id.brand_name",
      product_id: "$_id.product_id",
      top_feedbacks: 1
    }
  }
])

//za svaki skin_tone i skin_type i svaki produkt iz tertiary_category = tertiary_category naci brandove sa najjeftinijim prozivodima i izracunati prosecan total_pos_feedback_count ya njih

db.product_info.aggregate([
  {
    $unwind: "$feedback"
  },
  {
    $group: {
      _id: {
        skin_tone: "$feedback.skin_tone",
        skin_type: "$feedback.skin_type",
        tertiary_category: { $arrayElemAt: ["$tertiary_category", 0] }
      },
      min_price: { $min: "$price_usd" },
      avg_pos_feedback_count: { $avg: "$feedback.total_pos_feedback_count" }
    }
  },
  {
    $sort: {
      "_id.skin_tone": 1,
      "_id.skin_type": 1,
      "_id.tertiary_category": 1
    }
  }
])

//za svaki rating iz feedback-a i za svaki brand, naci po proizvod iz svake kategorije koji ima najvecu cenu
db.product_info.aggregate([
  {
    $unwind: "$feedback"
  },
  {
    $group: {
      _id: {
        rating: "$feedback.rating",
        brand_name: "$brand_name"
      },
      max_price: { $max: "$price_usd" }
    }
  },
  {
    $sort: {
      "_id.rating": 1,
      "_id.brand_name": 1
    }
  },
  {
    $group: {
      _id: "$_id.rating",
      highest_priced_products: {
        $push: {
          brand_name: "$_id.brand_name",
          max_price: "$max_price"
        }
      }
    }
  },
  {
    $project: {
      _id: 0,
      rating: "$_id",
      highest_priced_products: 1
    }
  }
])