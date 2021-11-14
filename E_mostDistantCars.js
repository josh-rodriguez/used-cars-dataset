use jrodriguez23_FinalProject;

// Print functions to format results from queries.
function northernCars(pipeline) {
  print("\n" + "---Most northern cars---");
  var cursor = db.carData.aggregate(pipeline);
  for (var i = 0; i < 1; i++) {
    if (!cursor.hasNext()) {
      break;
    }
    var city = cursor.next();
	// print(JSON.stringify(city))
    print(city.cityName + " is at " + city.nearestCityLongCoord + ", " + city.nearestCityLatCoord + " and has a car at " + city.locLongCoord.toFixed(6)+ ", " + city.locLatCoord.toFixed(6));
  }
};

function westernCars(pipeline) {
  print("\n" + "---Most western cars---");
  var cursor = db.carData.aggregate(pipeline);
  for (var i = 0; i < 3; i++) {
    if (!cursor.hasNext()) {
      break;
    }
    var city = cursor.next();
	// print(JSON.stringify(city));
    print(city.nearestCity.name + " is at " + city.nearestCityLongCoord.toFixed(6) + ", " + city.nearestCityLatCoord.toFixed(6) +  " and has a car at " + city.locLongCoord.toFixed(6) + ", " + city.locLatCoord.toFixed(6));
  }
};

// Aggregation Pipelines passed to print functions.

// Most northern cars and one distant city using $group, $project operators.  
northernCars([
 {
    $match: {
      state: "ca",
    },
  },
  { // Add fields for longitude and latitude coordinates for the car and city
    $addFields: {
      
      locLatCoord: { $arrayElemAt: ["$loc.coordinates", 1] },
      locLongCoord: { $arrayElemAt: ["$loc.coordinates", 0] },
	  nearestCityLatCoord: { $arrayElemAt: ["$nearestCity.loc.coordinates", 1] },
	  nearestCityLongCoord: { $arrayElemAt: ["$nearestCity.loc.coordinates", 0] }
    },
  },
  {
	$project: {
		_id:1,
		"locLatCoord":1,
		"locLongCoord":1,
		"nearestCityLatCoord":1,
		"nearestCityLongCoord":1,
		"cityName":"$nearestCity.name",
	}
  },
  {
    $sort: { // sort to find the most northern
      locLatCoord: 1,
	  },
  },
  {
    $group: { 
      _id: "$nearestCity.name",
	  car:{"$first":"$$ROOT"},
      totalDist: {
		$max: {$subtract: ["$nearestCityLatCoord", "$locLatCoord"]}
      },
    },
  },
  {
	  $replaceRoot: {
		  newRoot: { $mergeObjects: ["$car",{totalDist:"$totalDist"}]}
		},
  },
 ]);

// Most western cars, with cities sorted by most distant. More simple than previous aggregation pipeline.
westernCars([
 {
    $match: {
      state: "ca",
    },
  },
  { // Add fields for longitude and latitude coordinates for the car and city
    $addFields: {
		locLatCoord: { $arrayElemAt: ["$loc.coordinates", 1] },
		locLongCoord: { $arrayElemAt: ["$loc.coordinates", 0] },
		nearestCityLatCoord: { $arrayElemAt: ["$nearestCity.loc.coordinates", 1] },
		nearestCityLongCoord: { $arrayElemAt: ["$nearestCity.loc.coordinates", 0] },
		totalDist: {
		$subtract: [{ $arrayElemAt: ["$nearestCity.loc.coordinates", 0] }, { $arrayElemAt: ["$loc.coordinates", 0] }]}
		},
  },
  {
    $sort: { // sort to find the most western
      totalDist: 1,
	  },
  },
 ]);