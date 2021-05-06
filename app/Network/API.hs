
module Network.API where


import Data.Bits
import Data.Int
import Data.Word
import qualified Data.ByteString.Lazy as B

bs2w64 :: B.ByteString -> Word64
bs2w64 = B.foldl' (\x y -> x * 256 + fromIntegral y) 0

decodeLength :: B.ByteString -> Word64
decodeLength bs = bs2w64 len
  where sByte = B.head bs 
        leadZ = countLeadingZeros sByte
        lenB  = fromIntegral $ if leadZ == 8 then 0 else 1 + leadZ
        len   = B.take lenB $ (clearBit sByte ((8 - fromIntegral lenB))) `B.cons` (B.tail bs)


encodeLength :: B.ByteString -> Maybe B.ByteString
encodeLength bs = let len = B.length bs in
  let lenB = mkLenPrefix len in
  case (B.length $ lenB) > 7 of
    True  -> Nothing
    False -> do
      (h,t) <- B.uncons $ lenB
      let setB = fromIntegral $ 8 - (B.length lenB)
      case h `testBit` setB of
        True  -> return $ (bit $ setB - 1) `B.cons` h `B.cons` B.empty
        False -> return $ (h `setBit` setB) `B.cons` t

mkLenPrefix :: Int64 -> B.ByteString
mkLenPrefix 0   = B.empty
mkLenPrefix len = mkLenPrefix (len `shiftR` 8) `B.snoc` ((fromIntegral len) .&. 0xFF)
