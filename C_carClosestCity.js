use jrodriguez23_FinalProject;
// Note: script takes a few minutes to run. Didn't add timer to print progress.

//Find all CA Cars
var caCars = db.carData.find({ "state": "ca"}, {})

print(caCars.count());

while (caCars.hasNext()) {
  var car = caCars.next();

// find the closest city to each document in caCars result
  var closestCities = db.cityData.aggregate([
    {
      $geoNear: {
        near: {
          type: "Point",
          coordinates: [car.loc.coordinates[0], car.loc.coordinates[1]],
        },
        spherical: true,
        distanceField: "distance", //required field when using $geoNear aggregate pipeline stage
      },
    },
    {
      $sort: {
        distance: 1,
      },
    },
  ]);
  var closestCity = closestCities.next();

  db.carData.update({
    _id: car._id
  }, {
    $set: {
      "nearestCity.name": closestCity.Name,
      "nearestCity.loc": closestCity.loc
    }
  })
}