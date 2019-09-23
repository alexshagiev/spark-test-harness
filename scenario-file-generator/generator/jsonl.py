from enum import Enum
import random
import string
import logging
import datetime
from collections import OrderedDict

logger = logging.getLogger(__name__)


class Type(Enum):
    Date = 0
    String = 1
    BigInteger = 2
    BigDecimal = 3
    ArrayBigDecimal = 4
    Timestamp = 5
    Boolean = 6
    ArrayBigInteger = 7


def gen_random_name(i):
    name = ''.join(random.choice(string.ascii_lowercase) for i in range(15))
    if i > string.ascii_lowercase.__len__() - 1:
        i = i % string.ascii_lowercase.__len__()

    return string.ascii_lowercase[i] + "_" + name


def get_random_int():
    min_amt = -1000 * 1000 * 100
    max_amt = min_amt * -1
    return v_or_null(random.randint(min_amt, max_amt))


def get_random_float():
    return v_or_null(random.random())


def get_biz_date():
    return datetime.date.today()


def get_random_date():
    return get_biz_date() + datetime.timedelta(random.randint(-100, 100))
    # return time.time() + random.random()


def get_random_bool():
    return v_or_null(True if random.randint(0, 1) == 1 else False)


def get_random_ts():
    return datetime.datetime.now()


def get_random_str():
    return v_or_null(''.join(random.choice(string.ascii_uppercase) for i in range(random.randint(5, 25))))


def get_random_arr_int():
    return [random.randint(1, 1000) for i in range(1, random.randint(1, 3))]


def get_random_arr_dec():
    return [random.random() for i in range(1, random.randint(1, 3))]


def v_or_null(v):
    if random.randint(1, 10) % 3 == 0:
        return None
    else:
        return v


val_gen = {Type.Date: get_random_date, Type.String: get_random_str, Type.BigDecimal: get_random_float,
           Type.BigInteger: get_random_int, Type.ArrayBigDecimal: get_random_arr_dec, Type.Timestamp: get_random_ts,
           Type.Boolean: get_random_bool, Type.ArrayBigInteger: get_random_arr_int}


def get_random_column__name_2_types(column_num: int):
    column_name_2_type = OrderedDict()
    # fix first column to be biz date
    column_name_2_type['biz_date'] = Type.Date
    for i in range(column_num):
        type = Type(random.randint(1, Type.__len__() - 1))
        name = gen_random_name(i)
        # print('i={}- {}'.format(i, name))
        column_name_2_type[name] = type
    return column_name_2_type


def jsonl_header(dic: OrderedDict):
    header = '{ "metaData": ' + \
             '{"columnNames": ["' + '","'.join(dic.keys()) + '"] ' + \
             ', "types": ["' + '","'.join(
        dic[key].name if dic[key].name not in ('ArrayBigDecimal', 'ArrayBigInteger') else 'Array<BigDecimal>' if dic[
                                                                                                                     key].name == 'ArrayBigDecimal' else 'Array<BigInteger>'
        for key in dic) + '"]}' + \
             '}'
    return header


def jsonl_row(val: list, typ: list):
    def to_str(v, t):
        if v is None:
            return 'null'
        elif t in (Type.BigInteger, Type.BigDecimal):
            return v
        elif t == Type.Boolean:
            return 'true' if v else 'false'
        elif t in (Type.ArrayBigDecimal, Type.ArrayBigInteger):
            return 'null' if len(v) == 0 else v
        elif t == Type.Date:
            return '"{}"'.format(v.strftime("%Y%m%d"))
        elif t == Type.Timestamp:
            return '"{}"'.format(v.strftime("%Y%m%d-%H:%M:%S.%f"))
        else:
            return '"' + str(v) + '"'

    return '{"row":[' + \
           ','.join(str(to_str(val[i], typ[i])) for i in range(len(val))) + \
           ']}'


def write(file, cols: int = 10, rows: int = 10, rows_uniqueness_factor: int = 1):
    # column_num = col
    # row_num = row
    # repeat = repeat

    name_2_type = get_random_column__name_2_types(cols)
    # print(''.join(key + '-' + name_2_type[key].name + '\n' for key in name_2_type))

    header = jsonl_header(name_2_type)
    # logger.info(header)
    file.write(header + '\n')
    # generate random values for a rwo
    size = int(rows / rows_uniqueness_factor)
    for r in range(size):
        row = []
        for c in name_2_type:
            type = name_2_type[c]
            val = val_gen[type]()
            row.append(val)
            # print(c, val_gen[type](), type)
            row_str = jsonl_row(row, [name_2_type[key] for key in name_2_type])
        if (r + 1) % (size / 10) == 0 or r == size - 1:
            logger.info('Generated Line # {:,} of {:,}'.format((r + 1) * rows_uniqueness_factor, rows))
        for i in range(rows_uniqueness_factor):
            content = '{}\n'.format(row_str)
            file.write(content)
