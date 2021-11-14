use jrodriguez23_FinalProject;

function outputResults(direction, dirLetter, pipeline) {
  print("---Most " + direction + " cars---");
  var cursor = db.carData.aggregate(pipeline);
  for (var i = 0; i < 5; i++) {
    if (!cursor.hasNext()) {
      break;
    }
    var city = cursor.next();
    print(city._id + " has a car " + city.totalDist.toFixed(2) + " degrees " + dirLetter);
  }
  print("\n")
}

outputResults("northern", "N", [
  {
    $match: {
      state: "ca",
    },
  },
  {
    $addFields: {
      nearestCityCoord: { $arrayElemAt: ["$nearestCity.loc.coordinates", 1] },
      locCoord: { $arrayElemAt: ["$loc.coordinates", 1] },
    },
  },
  {
    $group: {
      _id: "$nearestCity.name",
      totalDist: {
        $max: {
          $subtract: ["$nearestCityCoord", "$locCoord"],
        },
      },
    },
  },
  {
    $sort: {
      totalDist: -1,
    },
  },
]);

outputResults("southern", "S", [
  {
    $match: {
      state: "ca",
    },
  },
  {
    $addFields: {
      nearestCityCoord: { $arrayElemAt: ["$nearestCity.loc.coordinates", 1] },
      locCoord: { $arrayElemAt: ["$loc.coordinates", 1] },
    },
  },
  {
    $group: {
      _id: "$nearestCity.name",
      totalDist: {
        $min: {
          $subtract: ["$nearestCityCoord", "$locCoord"],
        },
      },
    },
  },
  {
    $sort: {
      totalDist: 1,
    },
  },
]);

outputResults("eastern", "E", [
  {
    $match: {
      state: "ca",
    },
  },
  {
    $addFields: {
      nearestCityCoord: { $arrayElemAt: ["$nearestCity.loc.coordinates", 0] },
      locCoord: { $arrayElemAt: ["$loc.coordinates", 0] },
    },
  },
  {
    $group: {
      _id: "$nearestCity.name",
      totalDist: {
        $max: {
          $subtract: ["$nearestCityCoord", "$locCoord"],
        },
      },
    },
  },
  {
    $sort: {
      totalDist: -1,
    },
  },
]);

outputResults("western", "W", [
  {
    $match: {
      state: "ca",
    },
  },
  {
    $addFields: {
      nearestCityCoord: { $arrayElemAt: ["$nearestCity.loc.coordinates", 0] },
      locCoord: { $arrayElemAt: ["$loc.coordinates", 0] },
    },
  },
  {
    $group: {
      _id: "$nearestCity.name",
      totalDist: {
        $min: {
          $subtract: ["$nearestCityCoord", "$locCoord"],
        },
      },
    },
  },
  {
    $sort: {
      totalDist: 1,
    },
  },
]);
