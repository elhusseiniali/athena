import tokenize
import re

from operations import Read, Projection, Conversion, Assignment
from domain import ColumnDomain

import pprint
from copy import deepcopy


types = {
    "NAME": 1,
    "OP": 54,
    "STRING": 3,
    "NEWLINE": 4,
    "COMMENT": 60,
    "NL": 61,
    "ENDMARKER": 0
}

indices = {
    "type": 0,
    "string": 1,
    "start": 2,
    "end": 3,
    "line": 4
}

df_files = {}

SUPPORTED_TRANSFORMATION = ["to_datetime"]
dataframes = []
PANDAS_ALIAS = 'pd'

operation_list = []
analysis = {}


def get_substring(char1, char2, s):
    """Get substring of s from char1 to char2.

    Parameters
    ----------
    char1 : char
        _description_
    char2 : _type_
        _description_
    s : _type_
        _description_

    Returns
    -------
    _type_
        _description_
    """
    assert len(char1) == 1
    assert len(char2) == 1
    assert isinstance(s, str)
    return s[s.find(char1) + 1:s.find(char2) + 1]


def process_dataframe_operation(dataframe, op_name=None,
                                argument=None, column_name=None):
    result = ColumnDomain()

    if dataframe not in dataframes:
        dataframes.append(dataframe)
        # print(f"Added new dataframe: {dataframe}.")
    if op_name:
        if "join" in op_name:
            # set union for dataframe and argument
            # after checking that argument is a df
            # print(f"Joining {dataframe} and {argument}.")
            assert argument in dataframes
            result.current["may"] = analysis[dataframe].current["may"].\
                union(analysis[argument].current["may"])
            result.current["must"] = analysis[dataframe].current["must"].\
                union(analysis[argument].current["must"])
            result.original["may"] = analysis[dataframe].original["may"].\
                union(analysis[argument].original["may"])
            result.original["must"] = analysis[dataframe].original["must"].\
                union(analysis[argument].original["must"])
            result.added["may"] = analysis[dataframe].added["may"].\
                union(analysis[argument].added["may"])
            result.added["must"] = analysis[dataframe].added["must"].\
                union(analysis[argument].current["must"])
            result.removed["may"] = analysis[dataframe].removed["may"].\
                union(analysis[argument].removed["may"])
            result.removed["must"] = analysis[dataframe].removed["must"].\
                union(analysis[argument].removed["must"])

        elif "apply" in op_name:
            assert column_name
            # print(f"Apply a function: {op_name}({argument}).")
            analysis[dataframe].\
                current["must"].add(column_name)
            analysis[dataframe].\
                current["may"].add(column_name)
    else:
        # just simple column access
        print(dataframe, column_name)
        if column_name:
            analysis[dataframe].\
                current["must"].add(column_name)
            analysis[dataframe].\
                current["may"].add(column_name)
    return dataframe, op_name, argument, column_name, result


def process_pandas_call(statement):
    df_name = ''
    op_name = ''
    argument = ''
    col_name = ''
    if "to_datetime" in statement:
        op_name = "to_datetime"

        result = statement.replace("to_datetime", '')
        result = result.replace("(", '')
        result = result.replace(")", '')
        result = result.replace('\n', '')
        argument = result

        col_name = re.findall(r"'\s*([^']+?)\s*'", argument)

        df_name = (s.split(']')[-1] for s in argument.split('['))
        df_name = list(df_name)[0].replace(" ", '')
        if df_name not in dataframes:
            dataframes.append(df_name)

        if col_name:
            print(f"Converting column to date-time: {col_name}")
            for name in col_name:
                analysis[df_name].current["must"].add(name)
                analysis[df_name].current["must"].add(name)

    return df_name, op_name, argument, col_name


def process_left(variable):
    statements = variable.split(".", 1)
    for statement in statements:
        statement = statement.replace(" ", '')

    # try to use process_pandas_call
    if statement[0] == PANDAS_ALIAS + '.':
        print("Function call.")

    df_name = (s.split(']')[-1] for s in statements[0].split('['))
    df_name = list(df_name)[0].replace(" ", '')

    col_name = None
    op_name = None
    argument = None

    if '[' in statements[0] and ']' in statements[0]:
        col_name = statements[0][statements[0].find("[") + 1:
                                 statements[0].rfind("]")].replace("'", '')
    else:
        print("Just a possible dataframe, no column access.")

    if len(statements) == 2:
        # identify the operation and the argument
        full_op = list((s.split(']')[-1] for s in statements[1].split('[')))[0]

        op_name = (s.split(')')[-1] for s in full_op.split('('))
        op_name = list(op_name)[0].replace(" ", '')
        if op_name == 'loc':
            argument = re.findall(r'\[(.*)\]', statements[1])[0]

            if '>' in argument \
                or '<' in argument \
                    or '=' in argument \
                    or '!' in argument:
                elements = argument.split(',')
                print(elements)
                elements[0] = elements[0].rsplit('>', 1)[0]
                elements[0] = elements[0].rsplit('<', 1)[0]
                elements[0] = elements[0].rsplit('=', 1)[0]
                elements[0] = elements[0].rsplit('!', 1)[0]

                elements[0] = elements[0].replace(' ', '')
                elements[1] = elements[1].replace(' ', '')

                #   get inner df and column names
                print(elements[0])
                left_col = re.findall(r"'\s*([^']+?)\s*'", elements[0])[0]

                left_df = list(s.split(']')[-1]
                               for s in elements[0].split('['))[0]
                process_dataframe_operation(dataframe=left_df, op_name=None,
                                            argument=None,
                                            column_name=left_col)
                #   just get the column name
                #   column name here belongs to df_name from outer code
                right_col = elements[1].replace("[", '')
                right_col = right_col.replace("]", '')
                process_dataframe_operation(dataframe=df_name, op_name=None,
                                            argument=None,
                                            column_name=right_col)
        else:
            argument = full_op[full_op.find("(") + 1:
                               full_op.rfind(")")]
    process_dataframe_operation(df_name, op_name, argument, col_name)

    return df_name, col_name


def process_right(statement):
    """Process right-hand-side of a statement of the form A = B.

    Parameters
    ----------
    statement : (str)
        Right-hand-side statement.
        ADD ASSERT TO CHECK THAT IT'S A VALID PYTHON STATEMENT

    Returns
    -------
    _type_
        _description_
    """
    dataframe = ''
    op_name = ''
    argument = ''
    column_name = ''
    result = ''

    statements = statement.split(".", 1)
    statements[0] = statements[0].replace(" ", '')

    candidate = list(s.split(']')[-1] for s in statements[0].split('['))[0]
    # print("POSSIBLE DF: ", candidate)
    # geocode(df2['País'], provider='nominatim')['geometry']
    if "geocode" in candidate:
        temp = statement
        temp = temp.replace('geocode', '')
        first_arg = get_substring('(', ']', temp)
        inner_df = list((s.split(']')[-1]
                         for s in first_arg.split('[')))[0]
        cols = re.findall(r"'\s*([^']+?)\s*'", temp)
        inner_col = cols[0]
        outer_col = cols[-1]
        column_name = outer_col
        # outer col is for same inner_df
        dataframe, _, \
            _, _,\
            _ = process_dataframe_operation(inner_df,
                                            op_name=None,
                                            argument=None,
                                            column_name=inner_col)

        return dataframe, op_name, argument, column_name, result

    if PANDAS_ALIAS == candidate:
        # print("This is a function call, not a dataframe.")
        # print(statements, statements[1])
        dataframe, op_name,\
            argument, column_name = process_pandas_call(statements[1])
    elif candidate.isnumeric():
        #   could possibly raise a TypeError (or custom) exception
        #   then try-catch in the runner to process this
        print("Numeric :(")
    else:
        #   remember: could be of the sort A = some_var
        #   we need to add a check to see if this is a valid
        #   dataframe
        #   ideas: - check if already in dataframes
        #          - check if any operation (from SUPPORTED_TRANSFORMATIONS)
        #            is being applied
        #          - what else?
        dataframe = candidate
        if candidate not in dataframes:
            dataframes.append(candidate)

        if '[' in statements[0] and ']' in statements[0]:
            column_name = re.findall(r"'\s*([^']+?)\s*'", statements[0])
            if column_name:
                column_name = column_name[0]
        else:
            # print("no column access; just a dataframe")
            pass

        if candidate in dataframes:
            # whole if-block can be outside of this if
            # because we basically guarantee that candidate is in dataframes
            # on line 208
            dataframe = candidate
            # print("We have the dataframe ", dataframe)

            full_op = list((s.split(']')[-1]
                            for s in statements[1].split('[')))[0]
            # print("op: ", full_op)

            op_name = (s.split(')')[-1] for s in full_op.split('('))
            op_name = list(op_name)[0].replace(" ", '')
            # print("op_name: ", op_name)

            argument = full_op[full_op.find("(") + 1:
                               full_op.rfind(")")]
            # print("arg: ", argument)
            # print("dataframe: ", dataframe)

            _, _, _, _, result = process_dataframe_operation(dataframe,
                                                             op_name,
                                                             argument,
                                                             column_name)
        else:
            # print("What the hell is going on?")
            pass
    return dataframe, op_name, argument, column_name, result


def main(file_name='./data/guide-small.py'):
    with tokenize.open(file_name) as f:
        tokens = tokenize.generate_tokens(f.readline)
        old_line = ''
        for index, token in enumerate(tokens):
            current_line = token[indices["line"]].replace('"', "'")
            current_line = current_line.replace('\n', '')
            if token[indices["type"]] in (types["COMMENT"], types["NEWLINE"],
                                          types["NL"], types["ENDMARKER"]):
                continue
            if old_line == current_line:
                continue
            if "import " in current_line:
                continue
            elif ".head" in current_line:
                continue
            print(index, ": ", token)

            if "pd.read_csv" in current_line:
                df_name = token[indices["string"]]
                dataframes.append(df_name)

                file_name = re.findall(r"'\s*([^']+?)\s*'", current_line)
                df_files[df_name] = file_name
                operation_list.append(Read(file_name))

                analysis[df_name] = ColumnDomain()

            #   what if it isn't 2? then we have more than 1 equal per line
            #   this means that it's still of the form A = B, but there's some
            #   equal sign(s) to pass arguments probably
            #   should use the =1 arg in the split and check that the split
            #   is more than 1
            #   still a problem with df.dropna(inplace=True) for example
            #   could maybe check if LHS and RHS are well-formed
            #   use ast.parse(code), with SyntaxError exception
            #   https://stackoverflow.com/questions/11854745/how-to-tell-if-a-string-contains-valid-python-code
            elif len(current_line.split('=')) == 2:
                statements = current_line.split('=')

                left = statements[0]
                right = statements[1]

                #   process right-hand-side
                dataframe, op_name, argument, \
                    column_name, result = process_right(right)
                old = deepcopy(analysis)

                #   process left-hand-side

                #   need to verify that the LHS is a dataframe
                #   check that the RHS is a pandas statement
                #   or that LHS is in list of dataframes
                df_name, col_name = process_left(left)
                if df_name not in analysis.keys():
                    analysis[df_name] = ColumnDomain()

                if result:
                    analysis[df_name] = result

                #   else the operation could either be irrelevant (so continue)
                #   or ?

                if col_name:
                    if isinstance(col_name, str):
                        col_name = [col_name]
                    for name in col_name:
                        analysis[df_name].current["must"].add(name)
                        analysis[df_name].current["may"].add(name)
                        if name not in old[dataframe].current["must"]\
                            and name not in old[dataframe].current["may"]\
                            and name not in old[dataframe].original["must"]\
                                and name not in old[dataframe].\
                                original["may"]:
                            analysis[dataframe].added["may"].add(name)
            elif "geocode" in current_line:
                statements = current_line.split('=', 1)

                left = statements[0]
                right = statements[1]
                print(left, right)
                dataframe, op_name, argument, \
                    column_name, result = process_right(right)

                old = deepcopy(analysis)

                #   process left-hand-side

                #   need to verify that the LHS is a dataframe
                #   check that the RHS is a pandas statement
                #   or that LHS is in list of dataframes
                df_name, col_name = process_left(left)
                if df_name not in analysis.keys():
                    analysis[df_name] = ColumnDomain()

                if result:
                    analysis[df_name] = result

                #   else the operation could either be irrelevant (so continue)
                #   or ?

                if col_name:
                    if isinstance(col_name, str):
                        col_name = [col_name]
                    for name in col_name:
                        analysis[df_name].current["must"].add(name)
                        analysis[df_name].current["may"].add(name)
                        if name not in old[dataframe].current["must"]\
                            and name not in old[dataframe].current["may"]\
                            and name not in old[dataframe].original["must"]\
                                and name not in old[dataframe].\
                                original["may"]:
                            analysis[dataframe].added["may"].add(name)

            elif any(i in current_line
                     for i in dataframes):
                print("Operating on a dataframe")
            else:
                # one more case for pd. statements
                pass

            old_line = current_line

            result_file_name = 'output.txt'
            with open(result_file_name, 'w') as writer:
                for dataframe in analysis.keys():
                    writer.write("----")
                    writer.write(dataframe)
                    writer.write(": \n")
                    writer.write(pprint.pformat(str(analysis[dataframe])))
                    writer.write("\n\n")


if __name__ == "__main__":
    input_file = './data/guide-small.py'
    main(input_file)
    for key in analysis.keys():
        print(key, analysis[key])
