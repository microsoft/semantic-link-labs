from sempy_labs._helper_functions import convert_to_friendly_case


def test_convert_to_friendly_case():

    assert convert_to_friendly_case('MyNewTable34') == 'My New Table34'

    assert convert_to_friendly_case('Testing_my_new function') == 'Testing My New Function'

    assert convert_to_friendly_case('testingMyNewFunction') == 'Testing My New Function'
    assert convert_to_friendly_case(None) is None
