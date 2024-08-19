class PlanetaeBaseException(Exception):
    message: str | None = None

    def __init__(self, message: str | None = None, rewrite: bool = False) -> None:
        if rewrite:
            msg = message
        else:
            msg = self.name() + ": " + message if message else self.name()
        super().__init__(msg)

    @classmethod
    def name(cls):
        name = ""
        for character in cls.__name__:
            if character.isupper():
                name += " " + character.lower()
            else:
                name += character
        return name.rstrip("exception").strip().capitalize()


class CouldNotConnectWithDataPlanetaeBaseException(PlanetaeBaseException):
    pass
