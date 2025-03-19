from interloper.utils.strings import to_snake_case


class TestCamelToSnakeCase:
    def test_to_snake_case(self):
        assert to_snake_case("camelCase") == "camel_case"
        assert to_snake_case("CamelCase") == "camel_case"
        assert to_snake_case("CamelCamelCase") == "camel_camel_case"
        assert to_snake_case("CamelCAMELCase") == "camel_camel_case"
        assert to_snake_case("CAMELCamelCase") == "camel_camel_case"
        assert to_snake_case("CamelCamelCASE") == "camel_camel_case"
        assert to_snake_case("CAMELCamelCASE") == "camel_camel_case"
        assert to_snake_case("Camel1Case") == "camel_1_case"
        assert to_snake_case("1CamelCase") == "1_camel_case"
        assert to_snake_case("CamelCase1") == "camel_case_1"
        assert to_snake_case("Camel22Case") == "camel_22_case"
        assert to_snake_case("22CamelCase") == "22_camel_case"
        assert to_snake_case("CamelCase22") == "camel_case_22"
        assert to_snake_case("CamelCase1x") == "camel_case_1x"
        assert to_snake_case("Camel1x") == "camel_1x"
        assert to_snake_case("1xCamel") == "1x_camel"
        assert to_snake_case("Camel1X") == "camel_1_x"
        assert to_snake_case("CamelX1") == "camel_x1"
        assert to_snake_case("CamelX22") == "camel_x22"
        assert to_snake_case("Camel1XX") == "camel_1_xx"
        assert to_snake_case("Camel_X1") == "camel_x1"
        assert to_snake_case("Camel_X22") == "camel_x22"
        assert to_snake_case("Camel_x1") == "camel_x1"
        assert to_snake_case("Camel_x22") == "camel_x22"
