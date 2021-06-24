const fs = require('fs');
const csv = require('csv');
const { exit } = require('process');

const inputDirectoryFolder = '../EconomicTracker-main/data/';
const outputDirectoryFolder = '../EconomicTracker-main/data-processed/';

// const inputDirectoryFolder = './';
// const outputDirectoryFolder = './data-processed/';

fs.readdir(inputDirectoryFolder, (err, files) => {
  for (const file of files) {
    console.log(`Processing ${file}`);
    processFile(file);
  }
});

function processFile(file) {
  const parser = csv.parse();
  const transformer = csv.transform(function (data) {
    return data.map(function (value) { return value.toUpperCase() });
  });
  const stringifier = csv.stringify();
  let processedHeader = false;
  let stateFipsColumnIndex = -1;

  fs.createReadStream(`${inputDirectoryFolder}${file}`).pipe(parser);
  const fileWriter = fs.createWriteStream(`${outputDirectoryFolder}${file}`);

  parser.on('error', function(err) {
    console.log(err);
  });

  parser.on('end', function(err) {
    console.log(`Completed parsing ${file}`);
  });

  parser.on('readable', function () {
    while (data = parser.read()) {
      transformer.write(data);
    }
  });

  transformer.on('readable', function () {
    while (data = transformer.read()) {
      if (!processedHeader) {
        data.push('DATE');
        stateFipsColumnIndex = data.indexOf('STATEFIPS');
        data.push('STATE');
        processedHeader = true;
      } else {
        let month = data[1];
        if(month.length === 1) {
          month = `0${month}`;
        }
        let date = data[2];
        if(date.length === 1) {
          date = `0${date}`;
        }
        data.push(`${data[0]}/${month}/${date}`);
        if (stateFipsColumnIndex >= 0) {
          data.push(lookupFips(data[stateFipsColumnIndex]));
        }
      }
      stringifier.write(data);
    }
  });

  stringifier.on('readable', function () {
    while (data = stringifier.read()) {
      fileWriter.write(data);
    }
  });

  function lookupFips(fipsCode) {
    const STATE_FIPS_LOOKUP = {
      "10": "DE",
      "11": "DC",
      "12": "FL",
      "13": "GA",
      "15": "HI",
      "16": "ID",
      "17": "IL",
      "18": "IN",
      "19": "IA",
      "20": "KS",
      "21": "KY",
      "22": "LA",
      "23": "ME",
      "24": "MD",
      "25": "MA",
      "26": "MI",
      "27": "MN",
      "28": "MS",
      "29": "MO",
      "30": "MT",
      "31": "NE",
      "32": "NV",
      "33": "NH",
      "34": "NJ",
      "35": "NM",
      "36": "NY",
      "37": "NC",
      "38": "ND",
      "39": "OH",
      "40": "OK",
      "41": "OR",
      "42": "PA",
      "44": "RI",
      "45": "SC",
      "46": "SD",
      "47": "TN",
      "48": "TX",
      "49": "UT",
      "50": "VT",
      "51": "VA",
      "53": "WA",
      "54": "WV",
      "55": "WI",
      "56": "WY",
      "60": "AS",
      "66": "GU",
      "69": "MP",
      "72": "PR",
      "74": "UM",
      "78": "VI",
      "01": "AL",
      "02": "AK",
      "04": "AZ",
      "05": "AR",
      "06": "CA",
      "08": "CO",
      "09": "CT",
      "1": "AL",
      "2": "AK",
      "4": "AZ",
      "5": "AR",
      "6": "CA",
      "8": "CO",
      "9": "CT"
    };

    return STATE_FIPS_LOOKUP[fipsCode];

  }
}
