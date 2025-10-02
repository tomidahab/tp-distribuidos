
class DisplayTable:
    def __init__(self, header: list[(str, int)]):
        self.col_widths = header

    def display_header(self):
        # Print header
        col_widths = self.col_widths
        header_line = " | ".join(h.ljust(size) for h, size in col_widths)
        separator = "-+-".join('-' * size for _, size in col_widths)
        print(f"\033[1m{header_line}\033[0m", flush=True)
        print(separator, flush=True)

    def display_row(self, row_items):
        next_rows = []
        next_rows.append([""] * len(row_items))

        for i, item in enumerate(row_items):
            rest_item = str(item)
            i_line = 0
            while len(rest_item) > self.col_widths[i][1]:
                next_rows[i_line][i] = rest_item[:self.col_widths[i][1]]
                rest_item = rest_item[self.col_widths[i][1]:]
                if i_line == len(next_rows) - 1:
                    next_rows.append([""] * len(row_items))
                i_line+=1
            next_rows[i_line][i] = rest_item
            
        for items in next_rows:
            # print(row_items, flush=True)
            # print(items, flush=True)
            row_line = " | ".join(str(cell).ljust(self.col_widths[i][1]) for i, cell in enumerate(items))
            print(row_line, flush=True)
        
        separator = "-|-".join('-' * size for _, size in self.col_widths)
        print(separator, flush=True)