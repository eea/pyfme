from pyfme import Xlsx


def test_fake_xlsx():
    xlsx = Xlsx(name="Test")
    numbers = xlsx.read(1)
    assert numbers is not None
