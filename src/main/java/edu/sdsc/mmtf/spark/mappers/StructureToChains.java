package edu.sdsc.mmtf.spark.mappers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.decoder.DecoderUtils;
import org.rcsb.mmtf.encoder.AdapterToStructureData;

import scala.Tuple2;

/**
 * 
 */
public class StructureToChains implements PairFlatMapFunction<Tuple2<String,StructureDataInterface>,String, StructureDataInterface> {

	private static final long serialVersionUID = -920511132456209871L;

	public StructureToChains() {}
	
	
	@Override
	public Iterator<Tuple2<String, StructureDataInterface>> call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;

		// precalculate indices
		int numChains = structure.getChainsPerModel()[0];
		int[] chainToEntityIndex = getChainToEntityIndex(structure);
		int[] atomsPerChain = new int[numChains];
		int[] bondsPerChain = new int[numChains];
		getNumAtomsAndBonds(structure, atomsPerChain, bondsPerChain);
		
		List<Tuple2<String, StructureDataInterface>> chainList = new ArrayList<>();
	
		for (int i = 0, atomCounter = 0, groupCounter = 0; i < numChains; i++){	
			AdapterToStructureData newChain = new AdapterToStructureData();
			
			int entityToChainIndex = chainToEntityIndex[i];
			int polymerAtomCount = 0;

			Map<Integer, Integer> atomMap = new HashMap<>();

	        // to avoid of information loss, add chainName/IDs and entity id
			// this required by some queries
			String structureId = structure.getStructureId() + "." + structure.getChainNames()[i] +
					"." + structure.getChainIds()[i] + "." + (entityToChainIndex+1);
			
			// set header
			newChain.initStructure(bondsPerChain[i], atomsPerChain[i], 
					structure.getGroupsPerChain()[i], 1, 1, structureId);
			DecoderUtils.addXtalographicInfo(structure, newChain);
			DecoderUtils.addHeaderInfo(structure, newChain);	

			// set model info (only one model: 0)
			newChain.setModelInfo(0, 1);

			// set entity and chain info
			newChain.setEntityInfo(new int[]{0}, structure.getEntitySequence(entityToChainIndex), 
					structure.getEntityDescription(entityToChainIndex), structure.getEntityType(entityToChainIndex));
			newChain.setChainInfo(structure.getChainIds()[i], structure.getChainNames()[i], structure.getGroupsPerChain()[i]);

			for (int j = 0; j < structure.getGroupsPerChain()[i]; j++, groupCounter++){
				int groupIndex = structure.getGroupTypeIndices()[groupCounter];
				// set group info
				newChain.setGroupInfo(structure.getGroupName(groupIndex), structure.getGroupIds()[groupCounter], 
						structure.getInsCodes()[groupCounter], structure.getGroupChemCompType(groupIndex), structure.getNumAtomsInGroup(groupIndex),
						structure.getGroupBondOrders(groupIndex).length, structure.getGroupSingleLetterCode(groupIndex), structure.getGroupSequenceIndices()[groupCounter], 
						structure.getSecStructList()[groupCounter]);

				for (int k = 0; k < structure.getNumAtomsInGroup(groupIndex); k++, atomCounter++){
					// set atom info
					atomMap.put(atomCounter, polymerAtomCount);
					polymerAtomCount++;
					
					newChain.setAtomInfo(structure.getGroupAtomNames(groupIndex)[k], structure.getAtomIds()[atomCounter], structure.getAltLocIds()[atomCounter], 
							structure.getxCoords()[atomCounter], structure.getyCoords()[atomCounter], structure.getzCoords()[atomCounter], 
							structure.getOccupancies()[atomCounter], structure.getbFactors()[atomCounter], structure.getGroupElementNames(groupIndex)[k], structure.getGroupAtomCharges(groupIndex)[k]);
				
				}

				// add intra-group bond info
				for (int l = 0; l < structure.getGroupBondOrders(groupIndex).length; l++) {
					int bondIndOne = structure.getGroupBondIndices(groupIndex)[l*2];
					int bondIndTwo = structure.getGroupBondIndices(groupIndex)[l*2+1];
					int bondOrder = structure.getGroupBondOrders(groupIndex)[l];
					newChain.setGroupBond(bondIndOne, bondIndTwo, bondOrder);
					
				}
			}

			// Add inter-group bond info
			for(int ii = 0; ii < structure.getInterGroupBondOrders().length; ii++){
				int bondIndOne = structure.getInterGroupBondIndices()[ii*2];
				int bondIndTwo = structure.getInterGroupBondIndices()[ii*2+1];
				int bondOrder = structure.getInterGroupBondOrders()[ii];
				Integer indexOne = atomMap.get(bondIndOne);
				if (indexOne != null) {
					Integer indexTwo = atomMap.get(bondIndTwo);
					if (indexTwo != null) {
						newChain.setInterGroupBond(indexOne, indexTwo, bondOrder);
					}
				}
			}

			newChain.finalizeStructure();
			
			String chId = structure.getChainNames()[i];
			
			chainList.add(new Tuple2<String, StructureDataInterface>(structure.getStructureId() + "." + chId, newChain));
			
		}

		return chainList.iterator();
	}

	/**
	 * Gets the number of atoms and bonds per chain.
	 */
	private static void getNumAtomsAndBonds(StructureDataInterface structure, int[] atomsPerChain, int[] bondsPerChain) {
		int numChains = structure.getChainsPerModel()[0];

		for (int i = 0, groupCounter = 0; i < numChains; i++){	
			for (int j = 0; j < structure.getGroupsPerChain()[i]; j++, groupCounter++){
				int groupIndex = structure.getGroupTypeIndices()[groupCounter];
				atomsPerChain[i] += structure.getNumAtomsInGroup(groupIndex);
				bondsPerChain[i] += structure.getGroupBondOrders(groupIndex).length;
			}
		}
	}

	/**
	 * Returns an array that maps a chain index to an entity index.
	 * @param structureDataInterface
	 * @return
	 */
	private static int[] getChainToEntityIndex(StructureDataInterface structure) {
		int[] entityChainIndex = new int[structure.getNumChains()];

		for (int i = 0; i < structure.getNumEntities(); i++) {
			for (int j: structure.getEntityChainIndexList(i)) {
				entityChainIndex[j] = i;
			}
		}
		return entityChainIndex;
	}
}