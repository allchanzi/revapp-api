import concurrent.futures
import json
import numpy
import sklearn
import os
import re
import requests

### REVIEW SKEPTIC PART

REVIEW_SKEPTIC_URL = 'http://reviewskeptic.com/'

def avg_r(reviews: list) -> float:
    return sum([review.get('rating', 3) for review in reviews]) / len(reviews)

def mnr(product_data: list) -> tuple:
    dates = {}
    for review in product_data:
        if review.get('date', None) and review.get('date', None) in dates:
            dates[review.get('date')] += 1
        dates[review.get('date')] = 0

    return 'mnr', int(max(dates.values()))

def pr(product_reviews: list) -> tuple:
    return 'pr', len([rev for rev in product_reviews if rev.get('rating', 3) > 3]) / len(product_reviews)

def nr(product_reviews: list) -> tuple:
    return 'nr', len([rev for rev in product_reviews if rev.get('rating', 3) < 3]) / len(product_reviews)

def avg_rd(product: str, product_reviews: list, user_reviews: list) -> tuple:
    avg_r_product = avg_r(product_reviews)
    avg_r_user_product = avg_r([review for review in user_reviews if review.get('product') == product])
    return 'avg_rd', avg_r_user_product / avg_r_product

def wrd(product_data: list) -> tuple:
    return 'wrd', ""

def bst(product_data: list) -> tuple:
    return 'bst', ""

def rd(rating: int, product_reviews: list) -> tuple:
    avg_r_product = avg_r(product_reviews)
    return 'rd', rating / avg_r_product

def ext(rating=3) -> tuple:
    return 'ext', int(rating > 3)

def isr(user_reviews: list) -> tuple:
    return 'ist', len(user_reviews) < 2


def get_behavioral_data(inserted_review: dict, data: list) -> dict:
    rating = inserted_review.get('rating', 3)
    product = inserted_review.get('product')
    user = inserted_review.get('address')
    user_reviews = [review for review in data if review.get('address') == user]
    product_reviews = [review for review in data if review.get('product') == product]

    # behavioral_data_result = {r.get(): 0 for r in data}
    for review in data:
        action_list = [(mnr, (product_reviews, )), (pr, (product_reviews, )), (nr, (product_reviews, )),
                       (wrd, (product_reviews, )), (bst, (product_reviews, )), (rd, (rating, product_reviews, )), (ext, (rating, )), (isr, (user_reviews, )),
                       (avg_rd, (product, product_reviews, user_reviews))]
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(action[0], *action[1]) for action in action_list]
        return {f.result()[0]: f.result()[1] for f in futures}

def get_result_for_little_data(data: str) -> dict:
    request_data = {'review_text': data}
    res = requests.post(REVIEW_SKEPTIC_URL, request_data)
    regex_result   = r'<p class="analysis"[^>]*>\s*((?:.|\n)*?) </p>'
    regex_analysis = r'Result: <span class="analysis((?:.|\n)*?)"[^>]*>\s*(?:.|\n)*?<\/span>'
    return {'analysis': re.findall(regex_analysis, res.text)[0], 'result': re.findall(regex_result, res.text)[0]}

if __name__ == '__main__':
    with open('fileset/reviews.json', 'r') as file:
        review_json = json.loads(file.read())
    output = []
    for k, review in review_json.get("reviews").items():
        text_inserted_result = get_result_for_little_data(review["content"])
        output.append({"id": k, "inserted_review_text_output" : text_inserted_result.get('result'),
                       "inserted_review_value_output": int(text_inserted_result.get('analysis')),
                        "behavioral_data": get_behavioral_data(review, list(review_json.get("reviews").values()))})
    print(json.dumps(output))
