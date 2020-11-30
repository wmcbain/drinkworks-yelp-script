import yelp from "yelp-fusion";
import Airtable from "airtable";
import geodata from "./locale_data/st-louis.json";

console.log(`Geodata count: ${geodata.length}`);

const base = new Airtable({ apiKey: "key7LIGVqRFEU32nw" }).base(
  "app9vPaEUv7130Noo"
);

const client = yelp.client(
  "gDQ2i6V54aSz8CythySEPxkcP-ijej0JUY5A3tRpUqgGW2AWuFrXowi4aySq8TZz2rRBwZ5HpbTreyUJpagRPbsZlR6o5IymEEdz47jc0lx4rgLcSsvyWqvYb-ayX3Yx"
);

const getExistingAccounts = () =>
  new Promise((resolve, reject) => {
    const existingIds = [];
    base("Accounts")
      .select({
        view: "All accounts",
      })
      .eachPage(
        function page(records, fetchNextPage) {
          records.forEach(function (record) {
            existingIds.push(record.get("Yelp ID"));
          });
          fetchNextPage();
        },
        function done(err) {
          if (err) {
            reject();
          }
          resolve(existingIds);
        }
      );
  });

const maxRecords = 50;
const radius = 2500;

const getYelpBusinesses = (offset, coordinates) =>
  new Promise((resolve) => {
    client
      .search({
        categories:
          "beautysvc,artspacerentals,realestateagents,sharedofficespaces,hostels,hotels,interiordesign,graphicdesign,productdesign,web_design",
        latitude: coordinates[0],
        longitude: coordinates[1],
        radius: radius,
        limit: maxRecords,
        offset: offset,
      })
      .then((res) => {
        resolve({
          total: res.jsonBody.total,
          businesses: res.jsonBody.businesses,
          nextOffset: offset + maxRecords,
        });
      })
      .catch((error) => {
        console.error(error);
        resolve({ total: 0, businesses: [], nextOffset: 0 });
      });
  });

const publishAccounts = (accounts) =>
  new Promise((resolve) => {
    base("Accounts").create(accounts, (err) => {
      if (err) {
        console.error(err);
      }
      resolve();
    });
  });

(async () => {
  try {
    for (let k = 66; k < geodata.length; k++) {
      const existingYelpIDs = await getExistingAccounts();

      console.log(`Searching geodata number: ${k}`);
      const coordinates = geodata[k].fields.geopoint;
      if (!coordinates) continue;

      let offset = 0;
      let firstYelpResult = await getYelpBusinesses(offset, coordinates);
      console.log(`Total Yelp Results: ${firstYelpResult.total}`);
      let yelpBusinesses = firstYelpResult.businesses;
      offset = firstYelpResult.nextOffset;
      while (offset < firstYelpResult.total) {
        const nextYelpResult = await getYelpBusinesses(offset, coordinates);
        yelpBusinesses = yelpBusinesses.concat(nextYelpResult.businesses);
        offset = nextYelpResult.nextOffset;
      }

      const uniqueResults = yelpBusinesses.filter((business) => {
        if (!existingYelpIDs.includes(business.id)) {
          return business;
        }
      });

      for (let i = 0; i < uniqueResults.length; i += 10) {
        const section = uniqueResults.slice(i, i + 10);
        const mapped = section.map((business) => {
          const categories = business.categories.map(
            (category) => category.title
          );
          const address = business.location.display_address.reduce(
            (last, next, index) => `${last}${index !== 0 ? "\n" : ""}${next}`,
            ""
          );
          return {
            fields: {
              Name: business.name,
              "Business Categories": categories,
              "Yelp URL": business.url,
              Address: address,
              "Yelp ID": business.id,
              "Yelp Review Count": business.review_count,
              "Yelp Rating": business.rating,
              Phone: business.display_phone,
              "Yelp Price Score": business.price,
            },
          };
        });
        await publishAccounts(mapped);
      }
    }
  } catch (err) {
    console.error(err);
  }
})();
