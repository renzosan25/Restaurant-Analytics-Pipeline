from faker import Faker
import uuid
import random

faker = Faker("es_MX")

def generate_mock_places(lat, long, min_places=5, max_places=20):
    all_places = []
    place_counter = 1
    num_places = random.randint(min_places, max_places)
    center_lat, center_lng = lat, long
    for _ in range(num_places):
            delta_lat = random.uniform(-0.0003, 0.0003)
            delta_lng = random.uniform(-0.0003, 0.0003)
            place = {
                "id": str(uuid.uuid4()),
                "location": {
                    "latitude": round(center_lat + delta_lat, 8),
                    "longitude": round(center_lng + delta_lng, 8)
                },
                "rating": round(random.uniform(3.0, 5.0), 1),
                "userRatingCount": random.randint(0, 500),
                "displayName": {
                    "text": faker.company(),  # o faker.first_name() + ' ' + faker.word()
                    "languageCode": "es"
                },
                "delivery": random.choice([True, False])
                # Puedes añadir más campos aquí si quieres
            }

            all_places.append(place)
            place_counter += 1
    return {"places": all_places}