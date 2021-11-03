def update_padding_data(aa, fixed_length, padding_value=0.5):
    '''
    벡터화된 데이터를 파싱하여 패딩을 진행하는 작업
    모델을 통해 이상징후 및 점수 출력을 진행하기 위한 데이터 생성
    '''
    rows = []
    try:
        for a in aa:
            a_split = a.split(',')
            a = np.array(a_split)
            rows.append(np.pad(a, (0, 50), 'constant', constant_values=padding_value)[0:fixed_length])
    except Exception as ex:
        print('padding 실패 : ', ex)

    return np.concatenate(rows, axis=0).reshape(-1, fixed_length)
