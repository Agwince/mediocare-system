import math

def calculate_distance(lat1, lon1, lat2, lon2):
    """
    Calculates the distance in meters between two GPS coordinates
    using the Haversine formula.
    """
    # Radius of the Earth in meters
    R = 6371000 

    # Convert latitude and longitude from degrees to radians
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    # Haversine formula
    a = math.sin(delta_phi / 2.0)**2 + \
        math.cos(phi1) * math.cos(phi2) * \
        math.sin(delta_lambda / 2.0)**2
    
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    distance_in_meters = R * c
    return distance_in_meters

def verify_location(worker_lat, worker_lon, branch_lat, branch_lon, allowed_radius=15):
    """
    Checks if the worker is within the allowed 15-meter radius.
    """
    distance = calculate_distance(worker_lat, worker_lon, branch_lat, branch_lon)
    
    if distance <= allowed_radius:
        return True, f"Success! You are {distance:.2f} meters away from the branch."
    else:
        return False, f"Failed: You are {distance:.2f} meters away. You must be within {allowed_radius} meters to check in."

# --- Quick Test ---
if __name__ == '__main__':
    # Let's use the Westlands Branch coordinates from our database
    westlands_lat = -1.2650
    westlands_lon = 36.8000

    print("--- Testing GPS Logic ---")
    
    # Test 1: Worker is standing exactly at the branch
    print("\nTest 1: Worker at the exact location")
    success1, msg1 = verify_location(-1.2650, 36.8000, westlands_lat, westlands_lon)
    print(msg1)

    # Test 2: Worker is a few blocks away (Should fail)
    print("\nTest 2: Worker is far away")
    success2, msg2 = verify_location(-1.2660, 36.8050, westlands_lat, westlands_lon)
    print(msg2)