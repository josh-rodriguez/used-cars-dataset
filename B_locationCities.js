use jrodriguez23_FinalProject
​
//Convert lat and long into GeoJSON format for 2dsphere point.
db.cityData.updateMany(
    {},
    [{$set:{
		"loc.type": "Point",
        "loc.coordinates": ["$Longitude", "$Latitude"]
		}
    }]
);
​
//Create GeoJSON point
db.cityData.createIndex({"loc": "2dsphere"});

//Delete the original lat and long fields
db.cityData.updateMany({},
	{$unset:{
     "Latitude":"",
     "Longitude":"",
    }
});