import requests
import json 

api_key="YOUR_API_KEY"
api_url="https://places.googleapis.com/v1/places:searchNearby"

def api_call(lat,long):
  
  headers= {
      "Content-Type": "application/json",
      "X-Goog-FieldMask": "places.displayName,places.id,places.location,places.rating,places.userRatingCount,places.delivery",
      "X-Goog-Api-Key": api_key
  }
  
  content = {
    "includedTypes": ["restaurant"],
    "maxResultCount": 20,
    "rankPreference": "DISTANCE",
    "locationRestriction": {
      "circle": {
        "center": {
          "latitude": lat,
          "longitude": long},
        "radius": 250.0
      }
    }
  }
  print("Trying to connect to the Google API......")
  try:
      response = requests.post(api_url, headers=headers, json=content)
      response.raise_for_status()
      print("API response received successfully.")
      return response.json()
  except requests.exceptions.HTTPError as e:
    status_code = e.response.status_code
    if status_code == 429:
        print("Error 429: Query limit exceeded. Consider backing off or using exponential retry.")
        return None
    elif status_code == 403:
        print("Error 403: Forbidden. Your API key may be invalid or not authorized.")
        raise
    elif status_code == 400:
        print("Error 400: Bad request. Check your request formatting.")
        raise
    else:
        print(f"HTTP error occurred: {e}")
        raise
  except requests.exceptions.Timeout:
    print("Request timed out. Consider retrying or increasing timeout duration.")
    return None
  except requests.exceptions.ConnectionError:
    print("Connection error. Check your internet or proxy settings.")
    return None
  except requests.exceptions.RequestException as e:
    print(f"An error occurred: {e}")
    raise


def mock_api_call():
    return {
  "places": [
    {
      "id": "place_id_1",
      "location": {
        "latitude": 37.793832099999996,
        "longitude": -122.3966844
      },
      "rating": 3.9,
      "userRatingCount": 43,
      "displayName": {
        "text": "place_1",
        "languageCode": "usa"
      },
      "delivery": True
    },
    {
      "id": "place_id_2",
      "location": {
        "latitude": 37.7938006,
        "longitude": -122.39678669999999
      },
      "rating": 4.2,
      "userRatingCount": 442,
      "displayName": {
        "text": "place_2",
        "languageCode": "en"
      },
      "delivery": False
    },
    {
      "id": "place_id_3",
      "location": {
        "latitude": 37.793987800000004,
        "longitude": -122.39665000000001
      },
      "rating": 4.3,
      "userRatingCount": 1258,
      "displayName": {
        "text": "place_3",
        "languageCode": "en"
      },
      "delivery": False
    },
    {
      "id": "place_id_4",
      "location": {
        "latitude": 37.7938458,
        "longitude": -122.3969018
      },
      "rating": 4.4,
      "userRatingCount": 94,
      "displayName": {
        "text": "place_4",
        "languageCode": "en"
      },
      "delivery": False
    },
    {
      "id": "place_id_5",
      "location": {
        "latitude": 37.7943929,
        "longitude": -122.3967467
      },
      "rating": 4.5,
      "userRatingCount": 683,
      "displayName": {
        "text": "place_5",
        "languageCode": "en"
      },
      "delivery": True
    },
    {
      "id": "place_id_6",
      "location": {
        "latitude": 37.7943851,
        "longitude": -122.39607669999998
      },
      "rating": 4,
      "userRatingCount": 369,
      "displayName": {
        "text": "place_6",
        "languageCode": "en"
      },
      "delivery": False
    },
    {
      "id": "place_id_7",
      "location": {
        "latitude": 37.794337,
        "longitude": -122.397021
      },
      "rating": 4.2,
      "userRatingCount": 55,
      "displayName": {
        "text": "place_7",
        "languageCode": "en"
      },
      "delivery": True
    },
    {
      "id": "place_id_8",
      "location": {
        "latitude": 37.7938439,
        "longitude": -122.3974854
      },
      "rating": 3.8,
      "userRatingCount": 79,
      "displayName": {
        "text": "place_8",
        "languageCode": "en"
      },
      "delivery": True
    },
    {
      "id": "place_id_9",
      "location": {
        "latitude": 37.7932479,
        "longitude": -122.3974067
      },
      "rating": 3,
      "userRatingCount": 2,
      "displayName": {
        "text": "place_9",
        "languageCode": "en"
      },
      "delivery": False
    },
    {
      "id": "place_id_10",
      "location": {
        "latitude": 37.794473499999995,
        "longitude": -122.3969591
      },
      "rating": 4.4,
      "userRatingCount": 304,
      "displayName": {
        "text": "place_10",
        "languageCode": "en"
      },
      "delivery": True
    },
    {
      "id": "place_id_11",
      "location": {
        "latitude": 37.7942596,
        "longitude": -122.3974244
      },
      "rating": 3.7,
      "userRatingCount": 536,
      "displayName": {
        "text": "place_11",
        "languageCode": "en"
      },
      "delivery": True
    },
    {
      "id": "place_id_12",
      "location": {
        "latitude": 37.794610999999996,
        "longitude": -122.3960335
      },
      "rating": 4.9,
      "userRatingCount": 36,
      "displayName": {
        "text": "place_12",
        "languageCode": "en"
      },
      "delivery": False
    },
    {
      "id": "place_id_13",
      "location": {
        "latitude": 37.78821,
        "longitude": -122.39321
      },
      "rating": 4.4,
      "userRatingCount": 1675,
      "displayName": {
        "text": "place_13",
        "languageCode": "en"
      },
      "delivery": True
    },
    {
      "id": "place_id_14",
      "location": {
        "latitude": 37.78906,
        "longitude": -122.39175
      },
      "rating": 3.9,
      "userRatingCount": 61,
      "displayName": {
        "text": "place_14",
        "languageCode": "en"
      },
      "delivery": True
    },
    {
      "id": "place_id_15",
      "location": {
        "latitude": 37.80221,
        "longitude": -122.40291
      },
      "rating": 4.3,
      "userRatingCount": 4,
      "displayName": {
        "text": "place_15",
        "languageCode": "en"
      }
    },
    {
      "id": "place_id_16",
      "location": {
        "latitude": 37.79041,
        "longitude": -122.41273
      },
      "rating": 4.1,
      "userRatingCount": 96,
      "displayName": {
        "text": "place_16",
        "languageCode": "en"
      },
      "delivery": True
    },
    {
      "id": "place_id_17",
      "location": {
        "latitude": 37.80042,
        "longitude": -122.41149
      },
      "displayName": {
        "text": "place_17",
        "languageCode": "en"
      }
    },
    {
      "id": "place_id_18",
      "location": {
        "latitude": 37.79964,
        "longitude": -122.40638
      },
      "rating": 4.5,
      "userRatingCount": 42,
      "displayName": {
        "text": "place_18",
        "languageCode": "en"
      },
      "delivery": True
    },
    {
      "id": "place_id_19",
      "location": {
        "latitude": 37.79353,
        "longitude": -122.4075
      },
      "displayName": {
        "text": "place_19",
        "languageCode": "en"
      }
    },
    {
      "id": "place_id_20",
      "location": {
        "latitude": 37.790445,
        "longitude": -122.40076
      },
      "rating": 4.2,
      "userRatingCount": 2147,
      "displayName": {
        "text": "place_20",
        "languageCode": "en"
      },
      "delivery": True
    }
  ]
}
