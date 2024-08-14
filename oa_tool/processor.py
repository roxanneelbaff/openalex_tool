


def from_inverted_to_text(abstracted_inverted_index):
    word_index = []
    for k, v in abstracted_inverted_index.items():
        for index in v:
            word_index.append([k, index])
    word_index = sorted(word_index, key=lambda x: x[1])
    abstract = " ".join([x[0] for x in word_index])
    return abstract
