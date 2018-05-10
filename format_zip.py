omit_val = 0
zip_code = ""
with open("zipcode.txt") as f:
    for line in f:
        text = line.strip()
        text_splited = text.split(' ')
        if omit_val:
            omit_val -=1
            continue
        if len(text_splited) == 1:
            zip_code = text_splited[0]
            omit_val = 2
        else:
            print(text+' '+zip_code)