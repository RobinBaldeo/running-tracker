from garminconnect import Garmin
import json

g = Garmin("GARMIN_EMAIL", "GARMIN_PASSWORD")
g.login()

# Export the tokens
tokens = {
    "oauth1": g.garth.oauth1_token.__dict__,
    "oauth2": g.garth.oauth2_token.__dict__,
}
print(json.dumps(tokens, default=str))