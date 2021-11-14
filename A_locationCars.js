use jrodriguez23_FinalProject

//Remove cars with null or blank locations (7448 documents) and put them in a collection called carDataNoLocation
//Drop collection if it exists
db.carDataNoLocation.drop();

//Match cars with no locations and move into new collection
db.carData.aggregate(
	{$match:{
		$or: [{"lat": null}, {"long": null}]
		}
	},
	{$out:"carDataNoLocation"}
)

//Delete the documents with null lat or long
db.carData.deleteMany({$or:[{ lat: null },{ long: null }]});

//Create the GeoJSON format
db.carData.updateMany(
    {},
    [{
		$set:{
        "loc.type": "Point",
        "loc.coordinates": ["$long", "$lat"]
		}
	}]
);

//Create the location-based Index
db.carData.createIndex({"loc" : "2dsphere"});

//Remove the original lat and long fields
db.carData.updateMany(
	{},
	{$unset:{
		"lat":"",
		"long":"",
		}
	}
);

