
def title_spell_check(title,tool):
    list_error = []
    matches = tool.check(title)
    for i in matches:
        if i.ruleIssueType == 'misspelling':
            list_error.append(title[i.offset:i.offset+i.errorLength])
    return list_error 


def fuzzy_extract(keyword,title):  
    keyword = keyword.lower()
    title = title.lower()
    kw = keyword.split(" ")
    
    count = len(kw)
    
    s2 = 0
    kwd2 = ""
    
    for k in kw:
        s1 = 0
        kwd1 = ""
        for match in find_near_matches(k, title, max_l_dist=1):
            match = match.matched
            index = fuzz.WRatio(match, k)
            if index>70 and index>s1:
                kwd1 = match
                s1 = index
                
        kwd2 = kwd2 + " " + kwd1
        if s2 == 0:
            s2 = s1
        else:
            s2 = (s2+s1)
    return (kwd2,s2/count)