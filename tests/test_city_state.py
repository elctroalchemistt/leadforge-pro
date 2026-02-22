from leadforge.utils.address import parse_city_state_from_address

def test_parse_city_state_us():
    city, state = parse_city_state_from_address("123 Main St, Chicago IL 60601, USA")
    assert city == "Chicago"
    assert state == "IL"