class MayMust():
    def __init__(self, may, must):
        self.may = may
        self.must = must


class ColumnDomain():
    def __init__(self):
        self.original = {"must": set(), "may": set()}
        self.current = {"must": set(), "may": set()}
        self.added = {"must": set(), "may": set()}
        self.removed = {"must": set(), "may": set()}

        self.copies = {}

    def was_added(self, input_string):
        return input_string in self.added['must'] or \
            input_string in self.added['may']

    def __bool__(self):
        return self.original["must"] != set() \
            and self.original["may"] != set() \
            and self.current["must"] != set() \
            and self.current["may"] != set()\
            and self.added["must"] != set() \
            and self.added["may"] != set()\
            and self.removed["must"] != set() \
            and self.removed["may"] != set()

    def __str__(self):
        return f"Original: {self.original}, Current: {self.current}, "\
               f"Added: {self.added}, Removed: {self.removed}, "\
               f"T: {self.copies}"
