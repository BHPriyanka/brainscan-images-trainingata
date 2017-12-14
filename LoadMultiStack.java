package parseTIFFImage;

import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class LoadMultiStack
{
    private static final Logger logger = LogManager.getLogger(LoadMultiStack.class);

    public static void main(final String args[]) {
    	try{
    		byte[] output1 = parseImage("./input/1_image.tiff", 100, 100);
    		byte[] output2 = parseImage("./input/1_dist.tiff", 100, 100);
    		StringBuilder sb = new StringBuilder();
    		for(int i =0; i< output1.length;i++){
    			sb.append(output1[i]);
    			sb.append(",");
    		}
    		
    		sb.append("~~");
    		for(int i =0; i< output2.length;i++){
    			sb.append(output2[i]);
    			sb.append(",");
    		}
    		//FileOutputStream out = new FileOutputStream("./output/1_dist");
    		
    		
    		PrintWriter out = new PrintWriter("./input/1_image.txt");
    		out.write(sb.toString());
    	}catch(Exception e){
    		
    	}
    	
    }
    
    public static byte[] parseImage(String imageName, int x, int y){
        List<String> imagePixels = new ArrayList<String>();
		try {
			final File imageFile = checkFile(imageName);
			final int xDim = x;
			final int yDim = y;
			final ImageReader reader = buildReader(imageFile);
			final int zDim = reader.getNumImages(true);
			//final int zDim = 1;
	        final byte imageBytes[] = new byte[xDim * yDim * zDim];

			for (int iz = 0; iz < zDim; iz++) {
				final BufferedImage image = reader.read(iz);
				final DataBuffer dataBuffer = image.getRaster().getDataBuffer();
		        final byte layerBytes[] = ((DataBufferByte)dataBuffer).getData();
		        
				//System.out.println(layerBytes.length + "    " + xDim * yDim);
				System.arraycopy(layerBytes, 0, imageBytes, iz * xDim * yDim, xDim * yDim);
			}

	        for (int iz = 0 ; iz < zDim ; iz++) {
		        for (int iy = 0 ; iy < yDim ; iy++) {
			        for (int ix = 0 ; ix < xDim ; ix++) {
			        	StringBuilder sb = new StringBuilder();
			        	sb.append(ix);
			        	sb.append(";");
			        	sb.append(iy);
			        	sb.append(";");
			        	sb.append(iz);
			        	//sb.append("~~");
			        	sb.append(";");
			        	int b = imageBytes[iz * yDim * xDim + iy * xDim + ix];
			        	sb.append(b);
			        	imagePixels.add(sb.toString());
			        }
		        }
	        }
	        
			return imageBytes;

		} catch (final Exception e) {
			logger.error("", e);
			System.exit(1);
		}
		return null;
	}

    public static File checkFile(final String fileName) throws Exception {
    	final File imageFile = new File(fileName);
    	if (!imageFile.exists() || imageFile.isDirectory()) {
    		throw new Exception ("Image file does not exist: " + fileName);
    	}
    	return imageFile;
    }

    public static ImageReader buildReader(final File imageFile) throws Exception {
		final ImageInputStream imgInStream = ImageIO.createImageInputStream(imageFile);
		if (imgInStream == null || imgInStream.length() == 0){
			throw new Exception("Data load error - No input stream.");
		}
		Iterator<ImageReader> iter = ImageIO.getImageReaders(imgInStream);
		if (iter == null || !iter.hasNext()) {
			throw new Exception("Data load error - Image file format not supported by ImageIO.");
		}
		final ImageReader reader = iter.next();
		iter = null;
		reader.setInput(imgInStream);
		//int numPages;
		/*if ((numPages = reader.getNumImages(true)) != zDim) {
			throw new Exception("Data load error - Number of pages mismatch: " + numPages + " expected: " + zDim);
		}*/
		return reader;
    }
}