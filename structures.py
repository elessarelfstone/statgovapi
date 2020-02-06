import attr


def common_corrector(value):
    return '' if value is None else value


def bool_corrector(value):
    return str(value) if isinstance(value, bool) else value


@attr.s
class JuridicalInfo:
    bin = attr.ib(converter=common_corrector, default='')
    ip = attr.ib(converter=bool_corrector, default='')
    name = attr.ib(converter=common_corrector, default='')
    fio = attr.ib(converter=common_corrector, default='')
    katocode = attr.ib(converter=common_corrector, default='')
    krpcode = attr.ib(converter=common_corrector, default='')
    okedcode = attr.ib(converter=common_corrector, default='')
    registerdate = attr.ib(converter=common_corrector, default='')
    secondokeds = attr.ib(converter=common_corrector, default='')
    katoaddress = attr.ib(converter=common_corrector, default='')
    okedname = attr.ib(converter=common_corrector, default='')
    krpbfcode = attr.ib(converter=common_corrector, default='')
    krpbfname = attr.ib(converter=common_corrector, default='')
    katoid = attr.ib(converter=common_corrector, default='')
    krpname = attr.ib(converter=common_corrector, default='')


