package arch.data.types.bytearray;

public class ByteArray {
	public byte[] buffer = null;
	public int start = 0;
	public int end = 0;

        public void setBuffer(byte[] buffer) {
            this.buffer = buffer;
        }
        
        public void growBuffer(int sz) {
            byte[] b = new byte[sz];
            if (end >= start) {
                System.arraycopy(buffer, start, b, start, end - start);
            } else {
                System.arraycopy(buffer, start, b, start, buffer.length - start);
                if (b.length >= buffer.length + end) {
                    System.arraycopy(buffer, 0, b, buffer.length, end);
                    end += buffer.length;
                } else {
                    System.arraycopy(buffer, 0, b, buffer.length, b.length - buffer.length);
                    end -= (b.length - buffer.length);
                    System.arraycopy(buffer, b.length - buffer.length, b, 0, end);
                }
            }
            buffer = b;
        }
}
