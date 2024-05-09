

# redis에서 traceId poll


# 검색하면서 이어보기
    # trace_id가 해당하면서 error가 비어있지 않은 trace 검색
    # 거기의 parent id를 찾아서 다시 검색
    # parent_id가 0000인게 있을 때까지
        # 다 있을 경우 -> mongo
        # 다 없을 경우
        # -> 횟수 남으면 다시 redis에
        # -> 아니면 버려
