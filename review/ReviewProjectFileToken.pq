// Shared by both review loaders. Hash only a project name, never CSV contents.
// Pure M SHA-256 uses exact nonnegative 32-bit words and documented bitwise APIs.
// Needed because Power Query has no documented Binary.Hash implementation.
let
    fnXerCsvSha256 = (value as text) as text =>
        let
            Bytes = Binary.ToList(Text.ToBinary(value, TextEncoding.Utf8)),
            BitLength = List.Count(Bytes) * 8,
            Padding = Number.Mod(56 - Number.Mod(List.Count(Bytes) + 1, 64) + 64, 64),
            LengthBytes = List.Transform({7,6,5,4,3,2,1,0}, each Number.Mod(Number.IntegerDivide(BitLength, Number.Power(256, _)), 256)),
            Message = List.Buffer(List.Combine({Bytes, {128}, List.Repeat({0}, Padding), LengthBytes})),
            Mod32 = (n as number) as number => Number.Mod(n, 4294967296),
            Rotate = (n as number, bits as number) as number => Number.IntegerDivide(n, Number.Power(2, bits)) + Number.Mod(n, Number.Power(2, bits)) * Number.Power(2, 32 - bits),
            Xor3 = (a as number, b as number, c as number) as number => Number.BitwiseXor(Number.BitwiseXor(a, b), c),
            Small0 = (n as number) as number => Xor3(Rotate(n, 7), Rotate(n, 18), Number.IntegerDivide(n, 8)),
            Small1 = (n as number) as number => Xor3(Rotate(n, 17), Rotate(n, 19), Number.IntegerDivide(n, 1024)),
            Large0 = (n as number) as number => Xor3(Rotate(n, 2), Rotate(n, 13), Rotate(n, 22)),
            Large1 = (n as number) as number => Xor3(Rotate(n, 6), Rotate(n, 11), Rotate(n, 25)),
            Constants = List.Buffer({
                1116352408,1899447441,3049323471,3921009573,961987163,1508970993,2453635748,2870763221,
                3624381080,310598401,607225278,1426881987,1925078388,2162078206,2614888103,3248222580,
                3835390401,4022224774,264347078,604807628,770255983,1249150122,1555081692,1996064986,
                2554220882,2821834349,2952996808,3210313671,3336571891,3584528711,113926993,338241895,
                666307205,773529912,1294757372,1396182291,1695183700,1986661051,2177026350,2456956037,
                2730485921,2820302411,3259730800,3345764771,3516065817,3600352804,4094571909,275423344,
                430227734,506948616,659060556,883997877,958139571,1322822218,1537002063,1747873779,
                1955562222,2024104815,2227730452,2361852424,2428436474,2756734187,3204031479,3329325298}),
            Initial = {1779033703,3144134277,1013904242,2773480762,1359893119,2600822924,528734635,1541459225},
            ProcessBlock = (hash as list, block as number) as list =>
                let
                    Words16 = List.Transform({0..15}, (i) => List.Sum(List.Transform({0..3}, (j) => Message{block * 64 + i * 4 + j} * Number.Power(256, 3 - j)))),
                    Words = List.Buffer(List.Accumulate({16..63}, Words16, (w, i) => w & {Mod32(Small1(w{i - 2}) + w{i - 7} + Small0(w{i - 15}) + w{i - 16})})),
                    Compressed = List.Accumulate({0..63}, hash, (h, i) =>
                        let
                            Choose = Number.BitwiseXor(Number.BitwiseAnd(h{4}, h{5}), Number.BitwiseAnd(4294967295 - h{4}, h{6})),
                            Majority = Xor3(Number.BitwiseAnd(h{0}, h{1}), Number.BitwiseAnd(h{0}, h{2}), Number.BitwiseAnd(h{1}, h{2})),
                            T1 = Mod32(h{7} + Large1(h{4}) + Choose + Constants{i} + Words{i}),
                            T2 = Mod32(Large0(h{0}) + Majority)
                        in
                            {Mod32(T1 + T2), h{0}, h{1}, h{2}, Mod32(h{3} + T1), h{4}, h{5}, h{6}}),
                    Result = List.Transform({0..7}, each Mod32(hash{_} + Compressed{_}))
                in
                    List.Buffer(Result),
            Digest = List.Accumulate({0..Number.IntegerDivide(List.Count(Message), 64) - 1}, Initial, ProcessBlock),
            DigestBytes = List.Combine(List.Transform(Digest, (word) => List.Transform({3,2,1,0}, each Number.Mod(Number.IntegerDivide(word, Number.Power(256, _)), 256))))
        in
            Text.Upper(Binary.ToText(Binary.FromList(DigestBytes), BinaryEncoding.Hex)),
    fnXerCsvProjectFileToken = (projectCode as text) as text =>
        let
            Encoded = fnXerCsvProjectToken(projectCode)
        in
            if Text.Length(Encoded) <= 100 then Encoded else "~" & fnXerCsvSha256(projectCode)
in
    [Sha256 = fnXerCsvSha256, FileToken = fnXerCsvProjectFileToken]
